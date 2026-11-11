// Simulates a Ceph OSD that has the Faasm object class (src/cephclass) loaded.
//
// - The real cephclass.cpp is linked in, and called through the method pointer
//   it registers, exactly like the OSD does for `rados exec`.
// - SIGPIPE is blocked, as ceph's global_init does for every daemon.
// - Each object lives in its own placement group. Ceph holds the PG lock while
//   an object class method runs, so a stuck call blocks every later op on that
//   PG. We model that with one mutex per object.
// - A watchdog plays the role of Ceph's heartbeat map: an op thread busy for
//   more than --stuck-after seconds is reported, along with how much CPU it
//   burnt (a spinning thread sits at ~100%). With --suicide-after the process
//   aborts, like osd_op_thread_suicide_timeout.
//
// The request sent to the object class is built the same way as
// storageCallAndAwaitImpl in src/wavm/ndp_objects.cpp.

#include "cephcomm_generated.h"

#include <rados/objclass.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <random>
#include <string>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>

#include <flatbuffers/flatbuffers.h>
#include <spdlog/spdlog.h>

int g_cls_log_level = -1;

extern "C" void __cls_init();

namespace {

using Clock = std::chrono::steady_clock;

struct Options
{
    int threads = 4;
    double durationS = 10;
    int calls = 0; // if > 0, total number of calls instead of a duration
    int objects = 8;
    size_t objectBytes = 64 * 1024;
    int missingEvery = 0;    // every Nth call targets an object that is missing
    double stuckAfterS = 15; // osd_op_thread_timeout default
    double suicideAfterS = 0; // osd_op_thread_suicide_timeout (0 = off)
    double drainS = 20;
    bool verbose = false;
    std::string method = "maybe_exec_wasm_ro";
};

Options opts;

struct Method
{
    cls_method_cxx_call_t call;
    int flags;
};
std::map<std::string, Method> methods;

// Set while a class method runs: did it queue a write in the op's transaction?
thread_local bool opWrote = false;

struct ObjectState
{
    std::string name;
    std::string data;
    bool exists = true;
    std::mutex pgLock;
};

std::vector<std::unique_ptr<ObjectState>> objects;

struct OpThreadState
{
    std::atomic<int64_t> callStartNs{ 0 };
    std::atomic<bool> inCls{ false };
    std::atomic<bool> reported{ false };
    std::atomic<clockid_t> cpuClock{};
    std::atomic<int64_t> lastCpuNs{ 0 };
    std::atomic<int64_t> lastWallNs{ 0 };
};

std::vector<std::unique_ptr<OpThreadState>> opStates;

std::mutex statsMx;
std::map<std::string, int> outcomes;
int64_t maxLatencyMs = 0;
std::atomic<int> callsStarted{ 0 };
std::atomic<int> callsFinished{ 0 };
std::atomic<bool> stopIssuing{ false };

int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
             Clock::now().time_since_epoch())
      .count();
}

int64_t cpuNs(clockid_t c)
{
    timespec ts{};
    clock_gettime(c, &ts);
    return int64_t(ts.tv_sec) * 1'000'000'000 + ts.tv_nsec;
}

std::string makeObjectData(size_t bytes, unsigned seed)
{
    static const char* words[] = { "near ",  "data ",    "processing ",
                                   "ceph ",  "faasm ",   "storage ",
                                   "wasm ",  "funclet ", "offload ",
                                   "rados ", "\n" };
    std::mt19937 rng(seed);
    std::string out;
    out.reserve(bytes + 16);
    while (out.size() < bytes) {
        out += words[rng() % 11];
    }
    out.resize(bytes);
    return out;
}

std::vector<uint8_t> buildNdpRequest(uint64_t callId, const std::string& key)
{
    namespace ndpmsg = faasm::ndpmsg;
    flatbuffers::FlatBufferBuilder builder(256);
    std::vector<int32_t> wasmGlobals = { 4194160, 4199676, 4199676 };
    std::vector<int32_t> extraArgs;
    auto fBucket = builder.CreateString("ndp");
    auto fFunction = builder.CreateString("wordcount");
    auto fKey = builder.CreateString(key);
    auto fObjInfo = ndpmsg::CreateObjectInfo(builder, fBucket, fKey);
    auto fPyPtr = builder.CreateString("");
    auto fGlobals = builder.CreateVector(wasmGlobals);
    auto fArgs = builder.CreateVector(extraArgs.data(), extraArgs.size());
    auto fWasmInfo = ndpmsg::CreateWasmInfo(
      builder, fBucket, fFunction, 42, fPyPtr, fGlobals, fArgs);
    auto fOriginHost = builder.CreateString("compute-sim");
    auto ndpRequest = ndpmsg::CreateNdpRequest(
      builder, callId, fWasmInfo, fObjInfo, fOriginHost);
    builder.Finish(ndpRequest);
    return { builder.GetBufferPointer(),
             builder.GetBufferPointer() + builder.GetSize() };
}

void record(const std::string& outcome, int64_t latencyMs)
{
    std::lock_guard lock(statsMx);
    outcomes[outcome]++;
    maxLatencyMs = std::max(maxLatencyMs, latencyMs);
}

std::string classify(int rc, ceph::buffer::list& out, uint64_t callId)
{
    namespace ndpmsg = faasm::ndpmsg;
    if (rc < 0) {
        return std::string("cls returned ") + strerror(-rc);
    }
    std::string bytes(out.c_str(), out.length());
    flatbuffers::Verifier verifier(
      reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    if (!verifier.VerifyBuffer<ndpmsg::NdpResponse>(nullptr)) {
        return "invalid NdpResponse returned to compute node";
    }
    const auto* resp = flatbuffers::GetRoot<ndpmsg::NdpResponse>(bytes.data());
    if (resp->call_id() != callId) {
        return "mismatched call id returned to compute node";
    }
    switch (resp->result()) {
        case ndpmsg::NdpResult_Ok:
            return "OK (offloaded)";
        case ndpmsg::NdpResult_ProcessLocally:
            return "ProcessLocally";
        default: {
            std::string msg =
              resp->error_msg() ? resp->error_msg()->str() : std::string();
            // Strip call ids so identical errors group together
            auto pos = msg.find("call ");
            if (pos != std::string::npos) {
                auto end = msg.find(':', pos);
                if (end != std::string::npos) {
                    msg = msg.substr(0, pos) + "call N" + msg.substr(end);
                }
            }
            return "Error: " + msg;
        }
    }
}

void opThread(int idx, int callsForThisThread, uint64_t idBase)
{
    auto& st = *opStates[idx];
    clockid_t clock{};
    pthread_getcpuclockid(pthread_self(), &clock);
    st.cpuClock = clock;
    std::mt19937 rng(idx * 7919 + 1);
    const Method method = methods.at(opts.method);
    const auto deadline =
      Clock::now() + std::chrono::milliseconds(int64_t(opts.durationS * 1000));

    for (int i = 0;; i++) {
        if (opts.calls > 0 ? i >= callsForThisThread
                           : (Clock::now() >= deadline || stopIssuing)) {
            break;
        }
        int callNo = callsStarted.fetch_add(1) + 1;
        bool missing = opts.missingEvery > 0 && callNo % opts.missingEvery == 0;
        ObjectState& obj =
          missing ? *objects.back() : *objects[rng() % (objects.size() - 1)];
        uint64_t callId = (idBase + callNo) & 0x7FFFFFFF;

        auto reqBytes = buildNdpRequest(callId, obj.name);
        ceph::buffer::list in;
        in.append(reinterpret_cast<const char*>(reqBytes.data()),
                  reqBytes.size());
        ceph::buffer::list out;

        int64_t start = nowNs();
        st.reported = false;
        st.lastCpuNs = cpuNs(st.cpuClock);
        st.lastWallNs = start;
        st.callStartNs = start;
        int rc;
        {
            std::lock_guard pg(obj.pgLock);
            st.inCls = true;
            opWrote = false;
            rc = method.call(&obj, &in, &out);
            st.inCls = false;
            // PrimaryLogPG::execute_ctx: an op whose transaction isn't empty
            // must be a write op, and a class call only counts as one if the
            // method was registered with CLS_METHOD_WR.
            if (rc >= 0 && opWrote && !(method.flags & CLS_METHOD_WR)) {
                fprintf(stderr,
                        "[osd] PrimaryLogPG.cc: In function 'void "
                        "PrimaryLogPG::execute_ctx(OpContext*)': "
                        "ceph_assert(op->may_write() || op->may_cache()) "
                        "failed -> OSD aborts\n");
                fflush(stderr);
                _exit(134);
            }
        }
        st.callStartNs = 0;
        int64_t latencyMs = (nowNs() - start) / 1'000'000;
        record(classify(rc, out, callId), latencyMs);
        callsFinished++;
    }
}

void watchdog()
{
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        int64_t now = nowNs();
        for (size_t i = 0; i < opStates.size(); i++) {
            auto& st = *opStates[i];
            int64_t start = st.callStartNs.load();
            if (start == 0) {
                continue;
            }
            double busyS = (now - start) / 1e9;
            if (busyS >= opts.stuckAfterS && !st.reported.exchange(true)) {
                int64_t cpu = cpuNs(st.cpuClock);
                double cpuPct =
                  100.0 * (cpu - st.lastCpuNs) /
                  double(std::max<int64_t>(1, now - st.lastWallNs));
                fprintf(stderr,
                        "[osd] heartbeat_map is_healthy 'OSD::osd_op_tp "
                        "thread %zu' had timed out after %.0fs (%s, cpu "
                        "%.0f%%%s)\n",
                        i,
                        opts.stuckAfterS,
                        st.inCls ? "inside faasm cls method"
                                 : "waiting for PG lock held by a stuck op",
                        cpuPct,
                        cpuPct > 80 ? ": SPINNING" : "");
            }
            if (opts.suicideAfterS > 0 && busyS >= opts.suicideAfterS) {
                fprintf(stderr,
                        "[osd] heartbeat_map is_healthy 'OSD::osd_op_tp thread "
                        "%zu' had suicide timed out after %.0fs -> abort\n",
                        i,
                        opts.suicideAfterS);
                fflush(stderr);
                _exit(134);
            }
        }
    }
}

void usage()
{
    fprintf(stderr,
            "osd_sim [--threads N] [--duration S | --calls K] [--objects N]\n"
            "        [--object-bytes B] [--missing-every M]\n"
            "        [--stuck-after S] [--suicide-after S] [--drain S]\n"
            "        [--method ro|rw] [--verbose]\n");
}
}

// --- Fake objclass API used by cephclass.cpp ---

int cls_register(const char*, cls_handle_t* handle)
{
    *handle = nullptr;
    return 0;
}

int cls_register_cxx_method(cls_handle_t,
                            const char* method,
                            int flags,
                            cls_method_cxx_call_t call,
                            cls_method_handle_t* handle)
{
    methods[method] = Method{ call, flags };
    *handle = nullptr;
    return 0;
}

int cls_cxx_read(cls_method_context_t hctx,
                 int ofs,
                 int len,
                 ceph::buffer::list* bl)
{
    auto* obj = static_cast<ObjectState*>(hctx);
    if (!obj->exists) {
        return -ENOENT;
    }
    if (ofs < 0 || size_t(ofs) > obj->data.size()) {
        return 0;
    }
    size_t n = std::min(obj->data.size() - ofs, size_t(len));
    bl->append(obj->data.data() + ofs, n);
    return int(n);
}

int cls_cxx_write(cls_method_context_t hctx,
                  int ofs,
                  int len,
                  ceph::buffer::list* bl)
{
    auto* obj = static_cast<ObjectState*>(hctx);
    std::string bytes(bl->c_str(), bl->length());
    if (size_t(ofs) + bytes.size() > obj->data.size()) {
        obj->data.resize(ofs + bytes.size());
    }
    obj->data.replace(ofs, bytes.size(), bytes);
    (void)len;
    opWrote = true;
    return 0;
}

int main(int argc, char** argv)
{
    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) {
                usage();
                exit(2);
            }
            return argv[++i];
        };
        if (a == "--threads") {
            opts.threads = std::stoi(next());
        } else if (a == "--duration") {
            opts.durationS = std::stod(next());
        } else if (a == "--calls") {
            opts.calls = std::stoi(next());
        } else if (a == "--objects") {
            opts.objects = std::stoi(next());
        } else if (a == "--object-bytes") {
            opts.objectBytes = std::stoul(next());
        } else if (a == "--missing-every") {
            opts.missingEvery = std::stoi(next());
        } else if (a == "--stuck-after") {
            opts.stuckAfterS = std::stod(next());
        } else if (a == "--suicide-after") {
            opts.suicideAfterS = std::stod(next());
        } else if (a == "--drain") {
            opts.drainS = std::stod(next());
        } else if (a == "--method") {
            opts.method = "maybe_exec_wasm_" + next();
        } else if (a == "--verbose") {
            opts.verbose = true;
        } else {
            usage();
            return 2;
        }
    }

    // Ceph daemons block SIGPIPE (global_init), so writes to a closed socket
    // fail with EPIPE instead of killing the OSD.
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGPIPE);
    pthread_sigmask(SIG_BLOCK, &set, nullptr);

    // cephclass.cpp logs every step at info level; keep only perror() output
    // (which goes straight to stderr) unless asked
    spdlog::set_level(opts.verbose ? spdlog::level::info : spdlog::level::off);
    spdlog::set_pattern("[osd] [cls] %^[%l]%$ %v");
    if (opts.verbose) {
        g_cls_log_level = 1;
    }

    __cls_init();

    // N-1 real objects plus one name that does not exist
    for (int i = 0; i < opts.objects; i++) {
        auto obj = std::make_unique<ObjectState>();
        obj->name = "key" + std::to_string(i);
        obj->data = makeObjectData(opts.objectBytes, i);
        objects.push_back(std::move(obj));
    }
    objects.back()->name = "missing-key";
    objects.back()->exists = false;
    objects.back()->data.clear();

    for (int i = 0; i < opts.threads; i++) {
        opStates.push_back(std::make_unique<OpThreadState>());
    }

    std::thread(watchdog).detach();

    std::random_device rd;
    uint64_t idBase = rd() % 1'000'000'000;
    fprintf(stderr,
            "[osd] %s: %d op threads, %s, %d objects of %zu bytes\n",
            opts.method.c_str(),
            opts.threads,
            opts.calls > 0
              ? (std::to_string(opts.calls) + " calls").c_str()
              : (std::to_string(opts.durationS) + "s of load").c_str(),
            opts.objects - 1,
            opts.objectBytes);

    std::vector<std::thread> threads;
    for (int i = 0; i < opts.threads; i++) {
        int share = opts.calls > 0 ? opts.calls / opts.threads +
                                       (i < opts.calls % opts.threads)
                                   : 0;
        threads.emplace_back(opThread, i, share, idBase);
    }

    // Duration mode: issue calls for --duration, then give in-flight calls
    // --drain seconds to finish. Calls mode: --duration is the deadline for
    // all calls to finish.
    const auto loadEnd =
      Clock::now() + std::chrono::milliseconds(int64_t(opts.durationS * 1000));
    auto allDone = [&]() {
        int busy = 0;
        for (auto& st : opStates) {
            busy += st->callStartNs.load() != 0;
        }
        return busy == 0 && (opts.calls > 0 ? callsFinished.load() >= opts.calls
                                            : Clock::now() >= loadEnd);
    };
    while (!allDone() && Clock::now() < loadEnd) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    stopIssuing = true;
    if (opts.calls == 0) {
        const auto drainEnd =
          Clock::now() + std::chrono::milliseconds(int64_t(opts.drainS * 1000));
        while (!allDone() && Clock::now() < drainEnd) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    int stuck = 0;
    for (auto& st : opStates) {
        stuck += st->callStartNs.load() != 0;
    }

    {
        std::lock_guard lock(statsMx);
        fprintf(stderr, "\n[osd] ===== summary =====\n");
        fprintf(stderr,
                "[osd] calls started=%d finished=%d, max latency %lld ms\n",
                callsStarted.load(),
                callsFinished.load(),
                (long long)maxLatencyMs);
        for (auto& [k, v] : outcomes) {
            fprintf(stderr, "[osd]   %5d x %s\n", v, k.c_str());
        }
        fprintf(stderr,
                "[osd] op threads still stuck in a call: %d / %d\n",
                stuck,
                opts.threads);
    }
    fflush(stderr);
    // Stuck threads can't be joined, so exit hard.
    _exit(stuck > 0 ? 3 : 0);
}
