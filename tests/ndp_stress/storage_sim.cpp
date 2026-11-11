// Simulates the Faasm storage worker (pool_runner with IS_STORAGE_NODE=1).
//
// - The real src/wasm/ndp_endpoint.cpp is linked in and started through the
//   same asio start hook pool_runner uses, on an io_context shared by
//   ENDPOINT_NUM_THREADS threads (main thread included, like SIGNAL mode).
// - Scheduler::callFunction runs each offloaded call on its own executor
//   thread, then drops the extraData pointer once the task is done, which is
//   when faabric's Executor releases it (task = ExecutorTask()).
// - The "funclet" does what an NDP wordcount does on the storage side:
//   fetch the memory delta from the compute node, read its object through
//   __faasmndp_getMmap, compute, optionally write through __faasmndp_put.
//   The socket calls are the ones in src/wavm/ndp_objects.cpp.
// - SIGPIPE is left at its default action, as in pool_runner.

#include "cephcomm_generated.h"

#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/asio.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <wasm/ndp.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <dirent.h>
#include <exception>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <time.h>
#include <vector>

#include <flatbuffers/flatbuffers.h>

// ---- faabric stand-ins ----

namespace faabric::util {
SystemConfig::SystemConfig()
{
    isStorageNode = true;
    offload_cpu_threshold =
      std::stod(getEnvVar("OFFLOAD_CPU_THRESHOLD", "0.8"));
    offload_ram_threshold =
      std::stod(getEnvVar("OFFLOAD_RAM_THRESHOLD", "0.8"));
    offload_load_avg_threshold =
      std::stod(getEnvVar("OFFLOAD_LOAD_AVG_THRESHOLD", "0.8"));
    overrideCpuCount = std::stoi(getEnvVar("OVERRIDE_CPU_COUNT", "0"));
    endpointNumThreads = std::stoi(getEnvVar("ENDPOINT_NUM_THREADS", "4"));
}

SystemConfig& getSystemConfig()
{
    static SystemConfig conf;
    return conf;
}

faabric::Message messageFactory(const std::string& user,
                                const std::string& function)
{
    faabric::Message m;
    m.set_user(user);
    m.set_function(function);
    return m;
}
}

namespace faabric::scheduler {
Scheduler::Scheduler()
  : executionSlotsSemaphore(faabric::util::getUsableCores())
{
}

void Scheduler::callFunction(faabric::Message& msg,
                             bool forceLocal,
                             const MessageRecord& caller,
                             std::shared_ptr<void> extraData)
{
    (void)forceLocal;
    (void)caller;
    executeHook(msg, std::move(extraData));
}

Scheduler& getScheduler()
{
    static Scheduler sch;
    return sch;
}
}

// ---- simulator ----

namespace {

struct Options
{
    int deltaMs = 5;
    int computeMs = 20;
    size_t writeBytes = 0;
    int slowEvery = 0; // every Nth funclet stalls for slowMs after its read
    int slowMs = 0;
    int reportEveryS = 5;
};

Options opts;

std::atomic<int> inflight{ 0 };
std::atomic<int> started{ 0 };
std::atomic<int> succeeded{ 0 };
std::atomic<int> failed{ 0 };

int countFds()
{
    int n = 0;
    if (DIR* d = opendir("/proc/self/fd")) {
        while (readdir(d) != nullptr) {
            n++;
        }
        closedir(d);
    }
    return n - 2;
}

int64_t threadCpuNs()
{
    timespec ts{};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return int64_t(ts.tv_sec) * 1'000'000'000 + ts.tv_nsec;
}

// One request/response exchange with the OSD for the running call.
std::vector<uint8_t> storageRequest(uint64_t callId,
                                    flatbuffers::FlatBufferBuilder& builder)
{
#ifdef NDP_LEGACY_API
    // As in src/wavm/ndp_objects.cpp before the fix
    auto sock = faasm::getNdpSocketFromCall(callId);
    sock->sendMessage(builder.GetBufferPointer(), builder.GetSize());
    sock = nullptr;
    auto maybeResponse = faasm::awaitNdpResponse(callId);
    if (maybeResponse.has_value()) {
        return std::move(maybeResponse.value());
    }
    std::rethrow_exception(maybeResponse.error());
#else
    return faasm::ndpStorageRoundTrip(
      callId, std::span(builder.GetBufferPointer(), builder.GetSize()));
#endif
}

// __faasmndp_getMmap on the hinted object
std::vector<uint8_t> ndpRead(uint64_t callId, uint64_t offset, uint64_t len)
{
    namespace ndpmsg = faasm::ndpmsg;
    flatbuffers::FlatBufferBuilder builder(256);
    ndpmsg::NdpReadBuilder reqBuilder(builder);
    reqBuilder.add_offset(offset);
    reqBuilder.add_upto_length(len);
    auto reqOffset = reqBuilder.Finish();
    ndpmsg::StorageMessageBuilder msgBuilder(builder);
    msgBuilder.add_call_id(callId);
    msgBuilder.add_message_type(ndpmsg::TypedStorageMessage_NdpRead);
    msgBuilder.add_message(reqOffset.Union());
    auto msgOffset = msgBuilder.Finish();
    builder.Finish(msgOffset);
    return storageRequest(callId, builder);
}

// __faasmndp_put on the hinted object
void ndpWrite(uint64_t callId, const std::vector<uint8_t>& data)
{
    namespace ndpmsg = faasm::ndpmsg;
    flatbuffers::FlatBufferBuilder builder(256);
    auto dataField = builder.CreateVector(data.data(), data.size());
    ndpmsg::NdpWriteBuilder reqBuilder(builder);
    reqBuilder.add_offset(0);
    reqBuilder.add_data(dataField);
    auto reqOffset = reqBuilder.Finish();
    ndpmsg::StorageMessageBuilder msgBuilder(builder);
    msgBuilder.add_call_id(callId);
    msgBuilder.add_message_type(ndpmsg::TypedStorageMessage_NdpWrite);
    msgBuilder.add_message(reqOffset.Union());
    auto msgOffset = msgBuilder.Finish();
    builder.Finish(msgOffset);
    auto response = storageRequest(callId, builder);
    if (response.size() != 2 || response[0] != 'o' || response[1] != 'k') {
        throw std::runtime_error(
          "Unexpected response from CEPH storage operation");
    }
}

int runFunclet(const faabric::Message& msg, int taskNo)
{
    const uint64_t callId = msg.id();

    // WasmModule::executeTask: fetch the NDP memory delta from the compute
    // node (a FunctionCallClient round trip)
    std::this_thread::sleep_for(std::chrono::milliseconds(opts.deltaMs));

    auto data = ndpRead(callId, 0, 128 * 1024 * 1024);

    // Count words until we've used computeMs of CPU time
    int64_t cpuStart = threadCpuNs();
    uint64_t words = 0;
    do {
        bool inWord = false;
        for (uint8_t c : data) {
            bool space = c == ' ' || c == '\n';
            words += (!space && !inWord);
            inWord = !space;
        }
    } while (threadCpuNs() - cpuStart < int64_t(opts.computeMs) * 1'000'000);

    // A funclet that takes a long time between two storage messages, e.g. a
    // big object, a slow dependency or a CPU starved node
    if (opts.slowEvery > 0 && taskNo % opts.slowEvery == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(opts.slowMs));
    }

    if (opts.writeBytes > 0) {
        std::vector<uint8_t> out(opts.writeBytes, uint8_t('a' + words % 26));
        ndpWrite(callId, out);
    }
    return 0;
}

void executeTask(faabric::Message msg, std::shared_ptr<void> extraData)
{
    int taskNo = ++started;
    inflight++;
    std::thread([msg = std::move(msg),
                 extraData = std::move(extraData),
                 taskNo]() mutable {
        try {
            runFunclet(msg, taskNo);
            succeeded++;
        } catch (const std::exception& e) {
            failed++;
            SPDLOG_ERROR(
              "Task {} threw exception. What: {}", msg.id(), e.what());
        }
        // Executor: result is set, then the task (and its extraData) released
        extraData.reset();
        inflight--;
    }).detach();
}

void report(const char* prefix)
{
    fprintf(stderr,
            "[storage] %s funclets started=%d ok=%d failed=%d "
            "still-running=%d open-fds=%d\n",
            prefix,
            started.load(),
            succeeded.load(),
            failed.load(),
            inflight.load(),
            countFds());
}
}

int main(int argc, char** argv)
{
    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        std::string v = (i + 1 < argc) ? argv[i + 1] : "";
        if (a == "--delta-ms") {
            opts.deltaMs = std::stoi(v);
        } else if (a == "--compute-ms") {
            opts.computeMs = std::stoi(v);
        } else if (a == "--write-bytes") {
            opts.writeBytes = std::stoul(v);
        } else if (a == "--slow-every") {
            opts.slowEvery = std::stoi(v);
        } else if (a == "--slow-ms") {
            opts.slowMs = std::stoi(v);
        } else if (a == "--report-every") {
            opts.reportEveryS = std::stoi(v);
        } else {
            fprintf(stderr,
                    "storage_sim [--delta-ms N] [--compute-ms N] "
                    "[--write-bytes N] [--slow-every N --slow-ms N] "
                    "[--report-every S]\n");
            return 2;
        }
        i++;
    }

    spdlog::set_level(spdlog::level::err);
    spdlog::set_pattern("[storage] %^[%l]%$ %v");

    auto& sch = faabric::scheduler::getScheduler();
    sch.executeHook = executeTask;

    const int threadCount = faabric::util::getSystemConfig().endpointNumThreads;
    asio::io_context ioc(threadCount);

    // pool_runner: endpoint.addStartHook(faasm::getNdpEndpoint())
    faasm::getNdpEndpoint()(ioc);

    asio::signal_set signals(ioc, SIGINT, SIGTERM);
    signals.async_wait([&](const boost::system::error_code& ec, int) {
        if (!ec) {
            ioc.stop();
        }
    });

    std::jthread reporter([](std::stop_token st) {
        while (!st.stop_requested()) {
            for (int i = 0; i < opts.reportEveryS * 10 && !st.stop_requested();
                 i++) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            report("progress:");
        }
    });

    fprintf(stderr,
            "[storage] listening on %s with %d io threads, %u usable cores\n",
            std::string(faasm::DEFAULT_FAASM_CEPH_SOCKET_PATH).c_str(),
            threadCount,
            faabric::util::getUsableCores());

    std::vector<std::jthread> ioThreads;
    for (int i = 0; i < threadCount - 1; i++) {
        ioThreads.emplace_back([&]() { ioc.run(); });
    }
    ioc.run();
    for (auto& t : ioThreads) {
        t.join();
    }
    reporter.request_stop();
    reporter.join();
    report("final:");
    fflush(stderr);
    _exit(0);
}
