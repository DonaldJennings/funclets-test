# Notes on Near Data Processing in the Faasm-NDP project

## Ceph-Faasm communication via the unix domain socket

 - Faasm listens on a `SOCK_STREAM` UDS at `/run/faasm/faasm-ndp.sock`. See
   `ndp_stability.md` for the failure modes of this protocol and how to
   stress-test it
 - NDP calls are ran as follows:
   - Compute Faasm: rados_aio_exec (object, class="faasm", method="maybe_exec_wasm_ro")
   - Input sent: object path, calling node hostname
   - Storage Ceph: Receives the exec request, if busy replies with rejection, if not busy: sends a request on the Ceph-faasm socket for local processing
   - Storage Faasm: can reject the request or accept it. If accepted, sends a request for the module state to the compute node.
   - Storage Faasm-Ceph: while the operation executes, uses the UDS connection to exchange read/write commands as needed
   - Storage Faasm: when it finishes executing, sends a "finished" message over the UDS to terminate the Ceph operation
   - Storage Faasm: sends a direct result to the Compute Faasm runtime with the updated memory snapshot
   - Compute Faasm: restores the snapshot and continues execution.

