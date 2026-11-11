#pragma once

#include <cstdint>
#include <functional>
#include <span>
#include <vector>

#include <cephclass/cephcomm.h>
#include <faabric/util/asio.h>

namespace faasm {

// Asio start hook for the storage worker: accepts connections from the Ceph
// object class on the NDP unix socket.
std::function<void(asio::io_context&)> getNdpEndpoint();

// Sends a storage message (NdpRead/NdpWrite) for an offloaded call to the OSD
// that started it, and returns the OSD's reply. Calls for the same id are
// serialised. Throws std::runtime_error (or a subclass) if the call is not
// known, the OSD reported an error, or the connection failed or timed out.
std::vector<uint8_t> ndpStorageRoundTrip(uint64_t callId,
                                         std::span<const uint8_t> request);

}
