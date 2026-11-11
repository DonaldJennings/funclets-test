# NDP stability: the "bricked pipe" bug

This page explains why Faasm-NDP corrupted storage nodes under load, how to
reproduce it without a Ceph cluster, and what was changed to fix it.

## Summary

There was no single bug. The unix socket between the Ceph object class
(running inside each OSD) and the Faasm storage worker had several failure
modes that all ended the same way:

1. Something goes wrong on one NDP call: the OSD gives up after its 30 s
   poll, a read hits a missing object, a funclet writes more than ~200 KB, or
   a funclet writes back to its input object.
2. The storage worker dies (SIGPIPE, an uncaught `std::length_error`), or
   leaves a half-written message on the socket. When the write-back goes
   through the read-only method, it's the OSD itself that aborts.
3. Every OSD op thread that was talking to the worker then spins at 100% CPU
   forever (`recv()` returns 0 on a closed socket and the loop never exits),
   or blocks forever on a message that will never be completed. Those threads
   hold their PG locks, so every other op on those PGs queues behind them.
4. Ceph marks the OSD down, librados resends the op to the next replica, and
   the same thing happens there. OSD and PG state is persisted on the host
   (`/mnt/ceph`, `dev/container/shared_store`), so rebooting doesn't clear it.

"Works until a bit of load" follows from this: the 30 s timeout, slot
exhaustion and queueing only show up under concurrency. Separately, the
offload decision leaked a slot every time it said "busy", so after a handful
of busy readings a storage node stopped offloading for good.

All of this is reproducible with `tests/ndp_stress`, which compiles the real
`cephclass.cpp`, `cephcomm.h` and `ndp_endpoint.cpp` against small stand-ins
for Ceph and the faabric scheduler.

## How NDP works

```
Compute worker (WAVM)                Ceph OSD                     Storage worker
---------------------                --------                     --------------
__faasmndp_storageCallAndAwait
 storageCallAndAwaitImpl ──rados_aio_exec(faasm.maybe_exec_wasm_ro)──▶ cephclass.cpp
 (ndp_objects.cpp)                   connects to /run/faasm/faasm-ndp.sock ─▶ ndp_endpoint.cpp
                                     sends CephNdpRequest                     decides: offload?
                                                         ◀── NdpResponse(Ok | ProcessLocally)
                                                                              schedules funclet
 FunctionCallServer ◀──────────── requestNdpDelta (memory delta) ────────── WasmModule::executeTask
                                     ◀── NdpRead / NdpWrite ─────────────── __faasmndp_getMmap/_put
                                     ── data / "ok" ───────────────────────▶
                                     ◀── NdpEnd ─────────────────────────── funclet done
 ◀──────── NdpResponse (cls output) ─┘
 awaitChainedCallMessage ◀────────── direct result + memory delta ──────── Executor
 deltaRestore, continue
```

The OSD op thread, and the PG lock, are held from `rados_aio_exec` arriving
until `NdpEnd`, that is for the whole funclet.

### Where the NDP code lives

| Side | Files |
|---|---|
| Wire format | `src/cephclass/cephcomm.fbs`, `include/cephclass/cephcomm.h` |
| OSD (object class) | `src/cephclass/cephclass.cpp`, vendored Ceph headers in `include/rados/` |
| Storage worker | `src/wasm/ndp_endpoint.cpp`, `include/wasm/ndp.h`, hooked in by `src/runner/pool_runner.cpp` |
| Funclet and compute side | `src/wavm/ndp_objects.cpp`, `src/wasm/WasmModule.cpp` (`isstorage` delta fetch), `src/wasm/chaining_util.cpp`, `S3Wrapper::asyncNdpCall` in `src/storage/S3Wrapper.cpp` |
| faabric (`auto-ndp/faabric`, branch `donald-ug`) | `ExecutorTask::extraData`, `Scheduler::executionSlotsSemaphore`, `addLocalResultSlot`, `FunctionCallServer::registerNdpDeltaHandler`, `FaabricEndpoint::addStartHook`, `util/system_metrics.*`, offload thresholds in `SystemConfig` |
| Benchmarks | `clients/cpp/func/ndp/*.cpp` |
| Deployment | `docker-compose-cloudlab.yml`, `deploy/conf/ceph/ceph.conf`, `bin/run_ceph_osd.sh` |

### Diffing against upstream Faasm

- The fork started from upstream commit `891c2aa6` (v0.6.0+20, July 2021).
  The last upstream commit merged in is `7327c1df` (v0.7.6+75).
- `git diff 7327c1df HEAD -- src include` shows everything this fork changed
  (about 80 files). Most of `include/rados/` is vendored Ceph headers.
- faabric NDP changes: `git -C faabric log --oneline 675607d..origin/donald-ug`
  for the load-balancing work, plus `ee9f657`, `b5f8982`, `d3901c8`,
  `41ab5ba`, `546c3f0` and `e403138` for NDP plumbing.
- The `faabric` submodule pointer used to be `e403138`, which is older than
  the code here needs (`system_metrics.h`, offload thresholds). It now points
  at `donald-ug`.

## Learning plan

Read in this order, and run the harness as you go:

1. `docs/source/ndp.md` and the flow above, then `cephcomm.fbs`: the whole
   protocol is five message types.
2. `include/cephclass/cephcomm.h`: framing and socket IO, shared by both
   sides.
3. `src/cephclass/cephclass.cpp`: what the OSD does per call. Keep in mind
   it runs on an OSD op thread holding a PG lock.
4. `src/wasm/ndp_endpoint.cpp`: accepting calls, the offload decision, and
   how the funclet's reads and writes reach the OSD.
5. `src/wavm/ndp_objects.cpp`: the Wasm-facing intrinsics, both the compute
   side (`storageCallAndAwaitImpl`) and the storage side
   (`__faasmndp_getMmap`, `__faasmndp_put`).
6. faabric: `Executor::threadPoolThread` (when `extraData` is released),
   `Scheduler::callFunction`, `FunctionCallServer::recvNdpDeltaRequest`.
7. One benchmark end to end, e.g. `clients/cpp/func/ndp/wordcount.cpp`, then
   `substrrepl.cpp` (writes back to its input object).
8. Run every scenario in `tests/ndp_stress` against the old code (see below)
   and match each failure to the cause list.

## Root causes

Each item gives the symptom, the cause, the harness scenario that shows it,
and the fix.

### 1. OSD threads spin forever on a closed socket ("bricked pipe")

`rawRecvBytes` looped while `remaining > 0`, but `recv()` returns 0 when the
peer has closed the socket, so nothing ever changed. `pollFor` also reports a
closed socket as readable. Once the storage worker died or closed a
connection, every OSD thread reading from it burnt a core forever while
holding its PG lock. `EAGAIN` was handled the same way, as a busy loop.

*Scenarios:* `readerror`, `slowfunclet` (op threads at ~90-99% CPU, the
others stuck behind their PG locks).
*Fix:* `recv() == 0` throws `NdpSocketError`. All IO is non-blocking with a
`poll()` deadline.

### 2. SIGPIPE kills the storage worker (the EPIPE you remembered)

`send()` was called without `MSG_NOSIGNAL`, and nothing in faasm or faabric
ignores SIGPIPE. When the OSD gave up on a call (30 s with no message, or any
exception) and closed its end, the funclet's next write, usually `NdpEnd`,
killed the whole worker silently. In the OSD, where Ceph blocks SIGPIPE, the
same situation produces `Couldn't send cephcomm data: Broken pipe`.

*Scenario:* `slowfunclet`: worker "DIED from signal 13 (PIPE)", then cause 1
on every other in-flight call.
*Fix:* `MSG_NOSIGNAL` on every send, and `pool_runner` ignores SIGPIPE.

### 3. An OSD-side error crashes the storage worker

On a failed `cls_cxx_read` (for example a missing object) the OSD sends the
error marker, a length of `UINT64_MAX`. The worker's Asio handler called
`nextMsg.resize(UINT64_MAX)`. The resulting `std::length_error` escaped
`io_context::run()`, which is shared with the HTTP endpoint, and aborted the
process.

*Scenario:* `readerror`: one missing object out of 25 calls kills the worker,
and every OSD thread ends up stuck.
*Fix:* `recvMessageVector` checks for the error marker and a 6 GiB size cap
before allocating. The endpoint no longer uses Asio for reads, and nothing
can throw out of its handlers.

### 4. Large writes leave half a message on the wire

The worker gave the socket fd to Asio for `async_read`. Asio puts the fd into
non-blocking mode, while the funclet thread wrote to the same fd with plain
`send()`. Any write bigger than the socket buffer (~200 KB) got `EAGAIN`
part-way, threw, and left a partial frame. The OSD then blocked forever
waiting for the rest, and the worker never closed the socket (cause 6).

*Scenario:* `bigwrite` (4 MB `NdpWrite`): both OSD threads stuck at 0% CPU.
*Fix:* after the first readable event the fd is released from Asio. All IO is
synchronous with deadlines, and `EAGAIN` waits with `poll()`.

### 5. Writing to the input object aborts the OSD

The compute side always calls `maybe_exec_wasm_ro`, which is registered with
`CLS_METHOD_RD` only, and `readAllowed`/`writeAllowed` were never checked.
`substrrepl` writes back to its input object, so the class method calls
`cls_cxx_write`. In Ceph (v15 and v17 checked) that adds a write to a
read-only op's transaction, and `PrimaryLogPG::execute_ctx` hits
`ceph_assert(op->may_write() || op->may_cache())`: the OSD aborts. librados
resends the op to the next replica, which aborts as well.

*Scenario:* `rowrite` (the simulator models that assertion).
*Fix:* the object class enforces the method flags and fails such a write
with a clear error, without calling `cls_cxx_write`. **Decision for you:**
functions that write to their input object need `maybe_exec_wasm_rw`. See
"Still open".

### 6. Every offloaded call leaked a connection and an fd

`ndpSocketMap` held a `shared_ptr` to each `NdpConnection`, and the only code
that removed it was the `NdpConnection` destructor, which could never run.

*Scenarios:* `fdleak` (300 calls left 310 open fds), `load` (543).
*Fix:* an `NdpCallSession` is removed from the registry when the funclet
finishes (`NdpCallGuard`, released by the executor), and its socket is closed
then.

### 7. Funclets waited forever

`awaitNdpResponse` waited on an `atomic_flag` with no timeout, and connection
errors were only logged, never passed on. When the OSD gave up, the funclet
and its executor thread hung for good.

*Fix:* `ndpStorageRoundTrip` sends and receives on the funclet's own thread,
with a 60 s deadline, and throws on any error.

### 8. Two writers on one socket

The Asio thread sent the initial `NdpResponse` after `callFunction` had
already started the funclet, and the funclet wrote `NdpRead` on the same fd
from another thread with no lock. Under load the funclet could win the race,
or interleave its length header with the response.

*Fix:* the `Ok` response is sent before the funclet is scheduled. After that,
all IO for a call goes through the session's mutex.

### 9. Offloading switched itself off

- `getSystemUtilisation()` samples `/proc/stat` over 1 ms, but those
  counters tick every 10 ms, so the CPU figure is usually `0/0 = NaN`, and
  `NaN < threshold` is false.
- When the node had a free slot but the thresholds said "busy", the slot taken
  with `try_acquire()` was never released. After `cores` such decisions,
  `try_acquire()` always failed and the node answered `ProcessLocally` until
  it was restarted.

*Scenario:* `policy` (real readings, idle machine): old code offloads 8 of
200 calls, new code offloads 200 of 200.
*Fix:* CPU use is measured against the previous sample (at most every
100 ms). The slot is held for as long as the funclet runs and released on
every path.

### 10. Compute side

- `asyncNdpCall` returned the result of *submitting* the op
  (`rados_aio_exec`), not the object class result, so OSD-side errors were
  never seen. It also leaked the completion on error paths.
- The NDP delta handler captured a raw `WAVMWasmModule*` and was only removed
  on the success path. A late delta request would read a module that was
  already running something else. An exception inside the handler would also
  escape the `FunctionCallServer` thread.

*Fix:* return `rados_aio_get_return_value`. `NdpDeltaHandlerGuard` removes
the handler on every path, waits for a request that is already being served,
and never throws.

### 11. Ceph configuration

- `debug = 20` for every subsystem: under load this writes gigabytes of logs
  into the OSD containers on the host's root disk.
- The object class waits up to 30 s between messages, but
  `osd_op_thread_timeout` defaults to 15 s. Past that the OSD stops answering
  heartbeats and gets marked down (flapping), and at
  `osd_op_thread_suicide_timeout` it aborts.
- No `rados_osd_op_timeout`, so a stuck OSD hangs the calling compute thread
  forever.

*Fix:* `deploy/conf/ceph/ceph.conf`.

## Why it took an OS reinstall

The failures are in the Ceph layer, and its state lives on the host. Crashing
or flapping OSDs leave PGs down or peering, and `restart: on-failure`
replays the same ops into the same bug. With `debug = 20` the logs fill the
disk too. Rebooting brings back the same Ceph state; re-imaging the CloudLab
nodes wipes `/mnt/ceph`. On an affected cluster, `ceph -s`, `ceph health
detail` and `df -h` would have shown it.

## The screenshot

- `storageCallAndAwaitImpl reached a point where it was neither offloaded nor
  performed locally`: an old build logged this on every *successful* offload
  (the code fell through to it after `deltaRestore`). It was removed in
  `90f17290` and is harmless.
- `Error reading an HTTP request: bad method`: Beast rejecting a request
  whose first line isn't HTTP, i.e. something non-HTTP connecting to the
  worker's HTTP port (a TLS client, a port scanner, a health check). It isn't
  NDP traffic: the NDP socket is a separate unix socket, and the old code
  never closed NDP fds, so they weren't reused for HTTP connections. Logging
  the peer address in `HttpConnection::onRead` would confirm it.

## Reproducing it

```bash
# Ubuntu: apt install clang cmake ninja-build libboost-dev libflatbuffers-dev \
#         flatbuffers-compiler libspdlog-dev libfmt-dev libabsl-dev
git submodule update --init faabric

# Old code
mkdir -p /tmp/ndp-before
git archive 7280049 src/wasm/ndp_endpoint.cpp src/cephclass include/cephclass \
    include/wasm/ndp.h | tar -x -C /tmp/ndp-before
ln -s "$PWD/faabric" /tmp/ndp-before/faabric
CXX=clang++ cmake -S tests/ndp_stress -B /tmp/build-before -G Ninja \
    -DFAASM_ROOT=/tmp/ndp-before -DFAABRIC_ROOT=$PWD/faabric -DNDP_LEGACY_API=ON
cmake --build /tmp/build-before

# Current code
CXX=clang++ cmake -S tests/ndp_stress -B /tmp/build-after -G Ninja
cmake --build /tmp/build-after

# Needs write access to /run/faasm (the socket path is fixed in cephcomm.h)
sudo tests/ndp_stress/run.sh /tmp/build-before slowfunclet
sudo tests/ndp_stress/run.sh /tmp/build-after slowfunclet
```

Results on a 4-core VM. "Stuck" means OSD op threads still inside a call when
the run ended.

| Scenario | What it does | Before | After |
|---|---|---|---|
| `light` | 20 sequential calls | all OK | all OK |
| `policy` | 200 calls, real load readings, idle machine | 8 offloaded, then refused forever | 200 offloaded |
| `fdleak` | 300 sequential calls | 310 open fds | 10 open fds |
| `load` | 16 OSD threads for 20 s | all OK, 543 fds leaked | no leak; extra calls get `ProcessLocally` (cap = cores) |
| `readerror` | 1 in 25 calls reads a missing object | worker aborts (`length_error`), 4/4 OSD threads stuck, 3 spinning | 50 clean errors, worker fine |
| `bigwrite` | funclets write 4 MB (`_rw`) | half-sent frame, 2/2 OSD threads stuck forever | all OK |
| `rowrite` | funclets write back through `_ro` | OSD aborts on Ceph's assertion | clean error naming the fix |
| `noworker` | storage worker not running | each call holds an OSD op thread ~13 s (26 s with PG queueing), then fails | `ProcessLocally` in under 1 s (1.8 s with queueing) |
| `slowfunclet` | 1 in 20 funclets stalls for 32 s | worker killed by SIGPIPE, 4/4 OSD threads stuck, spinning | OSD times out the slow calls, nothing stuck |

The patched code gives no ThreadSanitizer reports (`light`, `readerror`,
`bigwrite`, `rowrite`, `load`, `slowfunclet`) and no AddressSanitizer or UBSan
reports (`fdleak`, `readerror`, `bigwrite`, `rowrite`, `load`). TSan did catch
one race in the new load-reading code during development; that's fixed.

## Behaviour changes

- **At most `usable cores` NDP funclets run at once on a storage node**
  (`OVERRIDE_CPU_COUNT` changes it). Calls above that get `ProcessLocally`.
  This is what the "has thread capacity" check was meant to do; before, it
  either did nothing or, after the slot leak, turned offloading off
  completely.
- If the storage worker isn't running, the object class answers
  `ProcessLocally` within about a second, instead of holding the OSD op
  thread for ~13 s and then failing the call.
- A write to the call's own object through `maybe_exec_wasm_ro` fails the
  call instead of crashing the OSD.
- `NdpEnd` now carries a result: if the funclet could not be scheduled after
  `Ok` was sent, the compute node gets an error instead of waiting.
- Logging: per-call NDP logs moved from info to debug, `debug = 20` is
  off, and `debug_objclass` is 1.

## Still open

- **Read-write NDP calls.** `storageCallAndAwaitImpl` always uses
  `maybe_exec_wasm_ro`, so `substrrepl` now fails cleanly under NDP instead of
  crashing the OSD. To make it work, call `maybe_exec_wasm_rw` for functions
  that write to their input object, for example through a new intrinsic or a
  flag on the call. Using `_rw` for everything also works (an op with an empty
  transaction completes like a read), but every NDP call then becomes a
  write-ordered op.
- **The design blocks an OSD op thread for the whole funclet.** If a funclet
  reads or writes another object in the same PG, or the same op shard, as its
  input (for example `damofilter` and `pcakmmstore`, which write a new key),
  the call deadlocks until the OSD's 30 s timeout. This is rare with 128 PGs
  per pool, but likely over thousands of calls. Possible fixes: write outputs
  through the NDP socket to the same object, write them after the call
  returns, or move NDP execution out of the op thread.
- **faabric bugs** (in `auto-ndp/faabric`, not changed here):
  - `Scheduler::getFunctionResult` waits with no timeout when a timeout is
    given (`if (!isBlocking) wait_for(...) else wait()` is the wrong way
    round). Polling a local result twice also throws, because
    `promise.get_future()` can only be called once; storing a
    `std::shared_future` in `MessageLocalResult` fixes both.
  - `FunctionCallServer::recvNdpDeltaRequest` answers a missing handler with
    an `EmptyResponse` where the client expects an `NdpDelta`.
  - An exception from any sync handler escapes the `MessageEndpointServer`
    thread.
  - `getSystemUtilisation()` has the 1 ms window problem from cause 9. Faasm
    no longer calls it.
- **Offload policy.** `docker-compose-cloudlab.yml` sets
  `OFFLOAD_LOAD_AVG_FACTOR_THRESHOLD`, but faabric reads
  `OFFLOAD_LOAD_AVG_THRESHOLD`, so the default of 0.8 applies. That threshold
  is compared with the raw 1-minute load average, so a 16-core node with one
  busy process already counts as overloaded. Dividing by the core count is
  probably what "factor" meant.
- **Not yet run on Ceph/CloudLab.** The harness runs the real socket code on
  both sides, but Ceph and the faabric scheduler are stand-ins. Before trusting
  new numbers: run `wordcount` and `substrrepl` (with `_rw`) under increasing
  load, and watch `ceph -s`, `ceph osd tree`, the OSD logs for
  `heartbeat_map` / `had timed out`, `ls /proc/<worker pid>/fd | wc -l`, and
  the share of `ProcessLocally` answers.
