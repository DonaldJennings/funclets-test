// Storage-side end of near-data processing (NDP).
//
// The Faasm object class running inside a Ceph OSD (src/cephclass) connects
// to this endpoint for every offloaded call and sends a CephNdpRequest. If the
// node has capacity we reply Ok and schedule the funclet locally; the funclet
// then reads/writes its object through the same connection
// (ndpStorageRoundTrip), and when it finishes we send NdpEnd and close the
// connection. Otherwise we reply ProcessLocally and the compute node runs the
// function itself.
//
// Concurrency rules:
// - Asio is only used to wait for a new connection to become readable. The fd
//   is then released from Asio, and all IO on it is synchronous with
//   deadlines (see cephcomm.h), under the NdpCallSession mutex.
// - The Ok response is sent before the funclet is scheduled, so the funclet's
//   first message can't overtake it on the wire.
// - The session lives from "Ok sent" until the funclet's task is released by
//   the executor (NdpCallGuard), and is then removed from the registry, so
//   connections and fds don't leak.
// - Nothing thrown here may escape into io_context::run(): that io_context is
//   shared with the HTTP endpoint, and an escaping exception aborts the
//   worker.

#include <cephcomm_generated.h>
#include <wasm/ndp.h>

#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/concurrent_map.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/system_metrics.h>

#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace faasm {

namespace {

namespace fbs = flatbuffers;

// How long to wait for the OSD's request once its connection is readable
constexpr int FIRST_MESSAGE_TIMEOUT_MS = 5000;
// How long a funclet waits for the OSD to answer one read or write
constexpr int STORAGE_REPLY_TIMEOUT_MS = 60000;
// Sends of small control messages (responses, NdpEnd)
constexpr int CONTROL_SEND_TIMEOUT_MS = 5000;

std::string str(const fbs::String* s)
{
    return s == nullptr ? std::string() : s->str();
}

std::vector<uint8_t> builderBytes(const fbs::FlatBufferBuilder& builder)
{
    return { builder.GetBufferPointer(),
             builder.GetBufferPointer() + builder.GetSize() };
}

std::vector<uint8_t> buildResponse(uint64_t callId,
                                   ndpmsg::NdpResult result,
                                   const std::string& error)
{
    fbs::FlatBufferBuilder builder(128 + error.size());
    auto errorField = builder.CreateString(error);
    ndpmsg::NdpResponseBuilder responseBuilder(builder);
    responseBuilder.add_call_id(callId);
    responseBuilder.add_result(result);
    responseBuilder.add_error_msg(errorField);
    builder.Finish(responseBuilder.Finish());
    return builderBytes(builder);
}

std::vector<uint8_t> buildEnd(uint64_t callId,
                              ndpmsg::NdpResult result,
                              const std::string& error)
{
    fbs::FlatBufferBuilder builder(128 + error.size());
    auto errorField = builder.CreateVector(
      reinterpret_cast<const uint8_t*>(error.data()), error.size());
    auto endField = ndpmsg::CreateNdpEnd(builder, result, errorField);
    auto endMsg = ndpmsg::CreateStorageMessage(
      builder, callId, ndpmsg::TypedStorageMessage_NdpEnd, endField.Union());
    builder.Finish(endMsg);
    return builderBytes(builder);
}

struct LoadReading
{
    double cpu;
    double ram;
    double loadAverage;
};

// faabric::util::getSystemUtilisation() measures CPU use over a 1 ms window,
// but /proc/stat only advances every 10 ms, so it mostly returns 0/0 = NaN,
// and `NaN < threshold` is always false. Measure CPU use since the previous
// sample instead, refreshing at most every 100 ms.
LoadReading readLoad()
{
    static std::mutex mx;
    static faabric::util::CPUStats last = faabric::util::getCPUUtilisation();
    static auto lastTime = std::chrono::steady_clock::now();
    static double cpu = 0.0;

    double cpuNow;
    {
        std::lock_guard lock(mx);
        auto now = std::chrono::steady_clock::now();
        if (now - lastTime >= std::chrono::milliseconds(100)) {
            faabric::util::CPUStats current =
              faabric::util::getCPUUtilisation();
            long total = current.totalCpuTime - last.totalCpuTime;
            long idle = current.idleCpuTime - last.idleCpuTime;
            if (total > 0) {
                cpu = std::clamp(1.0 - double(idle) / double(total), 0.0, 1.0);
                last = current;
                lastTime = now;
            }
        }
        cpuNow = cpu;
    }

    return { cpuNow,
             faabric::util::getMemoryUtilisation(),
             faabric::util::getLoadAverage() };
}

// One offloaded call: owns the connection to the OSD and one execution slot
// from the moment the call is accepted until the funclet is done.
class NdpCallSession : public std::enable_shared_from_this<NdpCallSession>
{
  public:
    explicit NdpCallSession(uint64_t callId)
      : callId(callId)
    {
    }

    NdpCallSession(const NdpCallSession&) = delete;
    NdpCallSession& operator=(const NdpCallSession&) = delete;

    // Every path that registers a session calls finish(), which removes it
    // from the registry; the destructor only closes the socket.
    ~NdpCallSession() = default;

    // Takes over the connection and the execution slot once the session is
    // in the registry
    void attach(std::unique_ptr<CephFaasmSocket> sock)
    {
        std::lock_guard lock(mx);
        socket = std::move(sock);
        registered = true;
        holdsSlot = true;
    }

    // Sends the Ok response. Returns false (and finishes the session) if the
    // OSD has already given up on the call.
    bool sendOk()
    {
        std::lock_guard lock(mx);
        try {
            auto bytes = buildResponse(callId, ndpmsg::NdpResult_Ok, "");
            socket->sendMessage(
              bytes.data(), bytes.size(), CONTROL_SEND_TIMEOUT_MS);
            return true;
        } catch (const std::exception& e) {
            SPDLOG_WARN(
              "NDP call {}: OSD went away before the call started: {}",
              callId,
              e.what());
            broken = true;
        }
        finishLocked(ndpmsg::NdpResult_Error, "");
        return false;
    }

    std::vector<uint8_t> roundTrip(std::span<const uint8_t> request)
    {
        std::lock_guard lock(mx);
        if (finished) {
            throw std::runtime_error("NDP call " + std::to_string(callId) +
                                     " has already finished");
        }
        if (broken) {
            throw std::runtime_error("NDP connection for call " +
                                     std::to_string(callId) +
                                     " failed earlier: " + brokenReason);
        }
        try {
            socket->sendMessage(
              request.data(), request.size(), STORAGE_REPLY_TIMEOUT_MS);
            return socket->recvMessageVector(STORAGE_REPLY_TIMEOUT_MS);
        } catch (const NdpPeerError&) {
            // The OSD sends its error marker and then abandons the call
            broken = true;
            brokenReason = "storage operation failed on the OSD";
            throw std::runtime_error("Storage operation for NDP call " +
                                     std::to_string(callId) +
                                     " failed on the OSD");
        } catch (const std::exception& e) {
            // We can't tell how much of the stream was used, so stop here
            broken = true;
            brokenReason = e.what();
            throw;
        }
    }

    // Tells the OSD the call is over, closes the connection, frees the
    // execution slot and removes the session from the registry. Only the
    // first call has any effect.
    void finish(ndpmsg::NdpResult result, const std::string& error)
    {
        std::lock_guard lock(mx);
        finishLocked(result, error);
    }

  private:
    void finishLocked(ndpmsg::NdpResult result, const std::string& error);

    const uint64_t callId;
    std::mutex mx;
    std::unique_ptr<CephFaasmSocket> socket;
    bool registered = false;
    bool holdsSlot = false;
    bool broken = false;
    bool finished = false;
    std::string brokenReason;
};

faabric::util::ConcurrentMap<uint64_t, std::shared_ptr<NdpCallSession>>
  ndpCalls;

void NdpCallSession::finishLocked(ndpmsg::NdpResult result,
                                  const std::string& error)
{
    if (finished) {
        return;
    }
    finished = true;

    // Free the slot before NdpEnd lets the OSD return, so that OSD's next
    // call can use it straight away
    if (holdsSlot) {
        holdsSlot = false;
        faabric::scheduler::getScheduler().executionSlotsSemaphore.release();
    }

    if (socket != nullptr && !broken) {
        try {
            SPDLOG_DEBUG("NDP call {}: sending NdpEnd", callId);
            auto bytes = buildEnd(callId, result, error);
            socket->sendMessage(
              bytes.data(), bytes.size(), CONTROL_SEND_TIMEOUT_MS);
        } catch (const std::exception& e) {
            // Normal if the OSD timed out or failed the call on its side
            SPDLOG_WARN(
              "NDP call {}: couldn't send NdpEnd: {}", callId, e.what());
        }
    }
    socket.reset();

    if (registered) {
        registered = false;
        ndpCalls.erase(callId);
    }
}

// Passed to the scheduler as the task's extraData. The executor drops it once
// the funclet has finished and its result has been set.
struct NdpCallGuard
{
    explicit NdpCallGuard(std::shared_ptr<NdpCallSession> session)
      : session(std::move(session))
    {
    }

    NdpCallGuard(const NdpCallGuard&) = delete;
    NdpCallGuard& operator=(const NdpCallGuard&) = delete;

    ~NdpCallGuard()
    {
        try {
            session->finish(ndpmsg::NdpResult_Ok, "");
        } catch (...) {
        }
    }

    std::shared_ptr<NdpCallSession> session;
};

// A new connection from the object class, until its request is handled
class NdpConnection : public std::enable_shared_from_this<NdpConnection>
{
  public:
    NdpConnection(asio::io_context& ioc, CephFaasmSocket&& sock)
      : socket(std::make_unique<CephFaasmSocket>(std::move(sock)))
      , waiter(asio::make_strand(ioc))
    {
        waiter.assign(asio::local::stream_protocol{}, socket->getFd());
    }

    NdpConnection(const NdpConnection&) = delete;
    NdpConnection& operator=(const NdpConnection&) = delete;

    ~NdpConnection() { releaseWaiter(); }

    void start()
    {
        waiter.async_wait(
          asio::local::stream_protocol::socket::wait_read,
          std::bind_front(&NdpConnection::onReadable, shared_from_this()));
    }

  private:
    // Asio only ever waits on this fd. Hand it back without closing it, so
    // Asio doesn't touch it (or its O_NONBLOCK flag) again.
    void releaseWaiter()
    {
        if (waiter.is_open()) {
            boost::system::error_code ec;
            waiter.release(ec);
        }
    }

    void onReadable(const boost::system::error_code& ec)
    {
        releaseWaiter();
        if (ec) {
            if (ec != asio::error::operation_aborted) {
                SPDLOG_ERROR("Error waiting for an NDP request: {}",
                             ec.message());
            }
            return;
        }
        try {
            handleRequest();
        } catch (const std::exception& e) {
            SPDLOG_ERROR("Error handling an NDP request: {}", e.what());
        } catch (...) {
            SPDLOG_ERROR("Unknown error handling an NDP request");
        }
    }

    void reply(uint64_t callId,
               ndpmsg::NdpResult result,
               const std::string& error)
    {
        try {
            auto bytes = buildResponse(callId, result, error);
            socket->sendMessage(
              bytes.data(), bytes.size(), CONTROL_SEND_TIMEOUT_MS);
        } catch (const std::exception& e) {
            SPDLOG_WARN(
              "Couldn't send NDP response for call {}: {}", callId, e.what());
        }
    }

    void handleRequest()
    {
        std::vector<uint8_t> requestData;
        try {
            requestData = socket->recvMessageVector(FIRST_MESSAGE_TIMEOUT_MS);
        } catch (const std::exception& e) {
            // The OSD closed the connection or never sent its request
            SPDLOG_WARN("Couldn't read NDP request: {}", e.what());
            return;
        }

        const ndpmsg::NdpRequest* request = nullptr;
        std::string osdName;
        try {
            verifyFlatbuf<ndpmsg::CephNdpRequest>(requestData);
            const auto* cephRequest =
              fbs::GetRoot<ndpmsg::CephNdpRequest>(requestData.data());
            request = cephRequest->request_nested_root();
            osdName = str(cephRequest->osd_name());
            if (request == nullptr || request->wasm() == nullptr ||
                request->object() == nullptr) {
                throw std::runtime_error("NDP request is missing fields");
            }
        } catch (const std::exception& e) {
            SPDLOG_ERROR("Invalid NDP request: {}", e.what());
            reply(0, ndpmsg::NdpResult_Error, e.what());
            return;
        }
        const uint64_t callId = request->call_id();

        faabric::Message msg;
        try {
            msg = buildMessage(*request);
        } catch (const std::exception& e) {
            SPDLOG_ERROR("Invalid NDP request {}: {}", callId, e.what());
            reply(callId, ndpmsg::NdpResult_Error, e.what());
            return;
        }

        auto& sch = faabric::scheduler::getScheduler();
        const auto& conf = faabric::util::getSystemConfig();

        // The slot is held for as long as the funclet runs, so at most
        // usable-cores NDP calls execute at once on this node
        const bool hasSlot = sch.executionSlotsSemaphore.try_acquire();
        const LoadReading load = readLoad();
        const bool belowThresholds =
          load.cpu < conf.offload_cpu_threshold &&
          load.ram < conf.offload_ram_threshold &&
          load.loadAverage < conf.offload_load_avg_threshold;
        const bool offload = hasSlot && belowThresholds;

        SPDLOG_DEBUG("NDP request {} from OSD {}: free slot={} cpu={:.2f} "
                     "ram={:.2f} load={:.2f} -> {}",
                     callId,
                     osdName,
                     hasSlot,
                     load.cpu,
                     load.ram,
                     load.loadAverage,
                     offload ? "offload" : "process locally");

        if (!offload) {
            if (hasSlot) {
                sch.executionSlotsSemaphore.release();
            }
            reply(callId, ndpmsg::NdpResult_ProcessLocally, "");
            return;
        }

        auto session = std::make_shared<NdpCallSession>(callId);
        if (!ndpCalls.tryEmplace(callId, session)) {
            sch.executionSlotsSemaphore.release();
            SPDLOG_ERROR("Duplicate NDP call id {}", callId);
            reply(callId, ndpmsg::NdpResult_Error, "Duplicate NDP call id");
            return;
        }
        // From here on the session owns the slot and the connection
        session->attach(std::move(socket));

        if (!session->sendOk()) {
            return;
        }

        auto guard = std::make_shared<NdpCallGuard>(session);
        try {
            sch.callFunction(msg, true, {}, guard);
        } catch (const std::exception& e) {
            SPDLOG_ERROR(
              "Exception when scheduling NDP call {}: {}", callId, e.what());
            session->finish(ndpmsg::NdpResult_Error,
                            std::string("Couldn't schedule the NDP call: ") +
                              e.what());
        }
    }

    static faabric::Message buildMessage(const ndpmsg::NdpRequest& request)
    {
        const auto* wasm = request.wasm();
        auto msg = faabric::util::messageFactory(str(wasm->user()),
                                                 str(wasm->function()));
        msg.set_id(request.call_id());
        msg.set_appid(request.call_id());
        msg.set_funcptr(wasm->fptr());
        const std::string pyFunction = str(wasm->pyptr());
        msg.set_ispython(!pyFunction.empty());
        msg.set_pythonfunction(pyFunction);
        msg.set_pythonuser(str(wasm->user()));
        msg.set_isstorage(true);
        msg.set_isoutputmemorydelta(true);
        msg.set_directresulthost(str(request.origin_host()));
        if (wasm->globals() != nullptr) {
            msg.mutable_wasmglobals()->Assign(wasm->globals()->cbegin(),
                                              wasm->globals()->cend());
        }
        if (wasm->args() != nullptr) {
            msg.mutable_extraarguments()->Assign(wasm->args()->cbegin(),
                                                 wasm->args()->cend());
        }
        msg.set_ndpcallobjectname(str(request.object()->key()));
        return msg;
    }

    std::unique_ptr<CephFaasmSocket> socket;
    asio::local::stream_protocol::socket waiter;
};

class NdpEndpoint : public std::enable_shared_from_this<NdpEndpoint>
{
  public:
    explicit NdpEndpoint(asio::io_context& ioc)
      : ioc(ioc)
      , socket(SocketType::listen)
      , acceptor(asio::make_strand(ioc))
      , retryTimer(acceptor.get_executor())
    {
        acceptor.assign(asio::local::stream_protocol{}, socket.getFd());
    }

    NdpEndpoint(const NdpEndpoint&) = delete;
    NdpEndpoint& operator=(const NdpEndpoint&) = delete;

    ~NdpEndpoint()
    {
        // CephFaasmSocket owns the fd
        boost::system::error_code ec;
        acceptor.release(ec);
    }

    void run()
    {
        asio::dispatch(
          acceptor.get_executor(),
          std::bind_front(&NdpEndpoint::doAccept, shared_from_this()));
    }

  private:
    void doAccept()
    {
        acceptor.async_wait(
          asio::local::stream_protocol::acceptor::wait_read,
          std::bind_front(&NdpEndpoint::onAcceptable, shared_from_this()));
    }

    void onAcceptable(const boost::system::error_code& ec)
    {
        if (ec == asio::error::operation_aborted) {
            return;
        }
        if (ec) {
            SPDLOG_ERROR("Error waiting for NDP connections: {}", ec.message());
            retryLater();
            return;
        }
        try {
            while (auto conn = socket.tryAccept()) {
                std::make_shared<NdpConnection>(ioc, std::move(*conn))->start();
            }
        } catch (const std::exception& e) {
            // e.g. out of file descriptors: back off instead of spinning, and
            // keep the endpoint alive
            SPDLOG_ERROR("Couldn't accept an NDP connection: {}", e.what());
            retryLater();
            return;
        }
        doAccept();
    }

    void retryLater()
    {
        retryTimer.expires_after(std::chrono::milliseconds(100));
        retryTimer.async_wait(
          [self = shared_from_this()](const boost::system::error_code& ec) {
              if (!ec) {
                  self->doAccept();
              }
          });
    }

    asio::io_context& ioc;
    CephFaasmSocket socket;
    asio::local::stream_protocol::acceptor acceptor;
    asio::steady_timer retryTimer;
};
}

std::function<void(asio::io_context&)> getNdpEndpoint()
{
    ndpCalls.reserve(512);
    // Take the first CPU sample now, so the first request gets a real reading
    readLoad();
    return
      [](asio::io_context& ioc) { std::make_shared<NdpEndpoint>(ioc)->run(); };
}

std::vector<uint8_t> ndpStorageRoundTrip(uint64_t callId,
                                         std::span<const uint8_t> request)
{
    auto session = ndpCalls.get(callId);
    if (!session.has_value()) {
        throw std::runtime_error("No NDP connection for call id " +
                                 std::to_string(callId));
    }
    return (*session)->roundTrip(request);
}

}
