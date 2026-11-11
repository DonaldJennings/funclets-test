# NDP socket stress harness

Runs the real NDP socket code on both ends, without Ceph or the rest of Faasm:

- `osd_sim` links the real `src/cephclass/cephclass.cpp` and calls it the way
  an OSD does. SIGPIPE is blocked as in Ceph daemons, each object has its own
  "PG lock", it aborts like Ceph when a read-only class method writes, and a
  watchdog reports op threads that are busy past `osd_op_thread_timeout`
  (15 s), including how much CPU they burn.
- `storage_sim` links the real `src/wasm/ndp_endpoint.cpp` and starts it the
  way `pool_runner` does, on a 4-thread io_context. Each offloaded call runs a
  funclet that fetches a "delta", reads its object, counts words and,
  optionally, writes back, using the same calls as `src/wavm/ndp_objects.cpp`.

`shim/` holds the stand-ins for Ceph's objclass API, the faabric scheduler and
Tracy. See `docs/source/ndp_stability.md` for what each scenario shows.

```bash
CXX=clang++ cmake -S tests/ndp_stress -B /tmp/build-ndp -G Ninja
cmake --build /tmp/build-ndp
sudo tests/ndp_stress/run.sh /tmp/build-ndp load
```

Scenarios: `light policy fdleak load readerror bigwrite rowrite noworker
slowfunclet`. Logs go to `<build>/logs`. The socket path is fixed at
`/run/faasm/faasm-ndp.sock`, so only one scenario can run at a time.

To build against the code before the fix, extract the old sources and pass
`-DFAASM_ROOT=<dir> -DFAABRIC_ROOT=$PWD/faabric -DNDP_LEGACY_API=ON` (see the
doc above). Add `-DCMAKE_CXX_FLAGS=-fsanitize=thread` (or
`address,undefined`) for sanitizer runs.
