#pragma once

#include "cephcomm_generated.h"

#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstring>
#include <endian.h>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

// Must compile in both C++20 and C++17 mode.
//
// Unix socket between the Ceph object class (running inside the OSD) and the
// Faasm storage runtime. Every message is a little-endian uint64 length
// followed by that many bytes. A length of NDP_ERROR_MARKER is an error
// report with no payload.
//
// All IO is non-blocking with poll() deadlines, so a peer that stalls or dies
// makes the call fail instead of hanging the thread. Sends use MSG_NOSIGNAL,
// so a peer that has gone away gives EPIPE rather than a SIGPIPE that kills
// the process.
//
// A CephFaasmSocket is not thread-safe: callers must serialise access.

namespace faasm {

inline const std::string_view DEFAULT_FAASM_CEPH_SOCKET_PATH =
  "/run/faasm/faasm-ndp.sock";

// Objects can be up to 5 GiB, so anything bigger is a corrupt length field.
constexpr uint64_t NDP_MAX_MESSAGE_SIZE = uint64_t(6) << 30;
constexpr uint64_t NDP_ERROR_MARKER = UINT64_MAX;
constexpr int NDP_DEFAULT_IO_TIMEOUT_MS = 30000;
constexpr int NDP_DEFAULT_CONNECT_TIMEOUT_MS = 1000;

// IO on the socket failed: the peer closed it, timed out, or sent garbage.
// The stream can't be used any more after this.
class NdpSocketError : public std::runtime_error
{
  public:
    using std::runtime_error::runtime_error;
};

// The peer sent the error marker instead of a reply.
class NdpPeerError : public std::runtime_error
{
  public:
    using std::runtime_error::runtime_error;
};

// The Faasm storage runtime isn't running or isn't accepting connections.
class NdpRuntimeUnavailable : public std::runtime_error
{
  public:
    using std::runtime_error::runtime_error;
};

enum class SocketType
{
    listen,
    connect
};

class CephFaasmSocket
{
  public:
    using Clock = std::chrono::steady_clock;

    explicit CephFaasmSocket(
      SocketType type,
      std::string_view path = DEFAULT_FAASM_CEPH_SOCKET_PATH,
      int connectTimeoutMs = NDP_DEFAULT_CONNECT_TIMEOUT_MS)
      : type(type)
      , bindAddr(path)
    {
        sockaddr_un addr = { AF_UNIX, { 0 } };
        if (path.size() >= sizeof(addr.sun_path)) {
            throw std::runtime_error("Path too long for socket");
        }
        size_t copied = path.copy(addr.sun_path, sizeof(addr.sun_path) - 1, 0);
        addr.sun_path[copied] = '\0';

        if (type == SocketType::listen) {
            ::mkdir("/run/faasm", 0777);
            fd =
              ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
            if (fd < 0) {
                throw NdpSocketError(errnoMessage("Couldn't create the "
                                                  "ceph-faasm socket"));
            }
            ::unlink(addr.sun_path);
            if (::bind(fd,
                       reinterpret_cast<const sockaddr*>(&addr),
                       sizeof(addr)) < 0) {
                std::string msg =
                  errnoMessage("Couldn't bind the ceph-faasm socket");
                closeFd();
                throw NdpSocketError(msg);
            }
            ::chmod(addr.sun_path, 0777);
            if (::listen(fd, 1024) < 0) {
                std::string msg =
                  errnoMessage("Couldn't listen on the ceph-faasm socket");
                closeFd();
                throw NdpSocketError(msg);
            }
            return;
        }

        connectWithRetry(addr, connectTimeoutMs);
    }

    CephFaasmSocket(const CephFaasmSocket&) = delete;
    CephFaasmSocket& operator=(const CephFaasmSocket&) = delete;

    CephFaasmSocket(CephFaasmSocket&& other) noexcept
      : type(other.type)
      , bindAddr(std::move(other.bindAddr))
      , fd(other.fd)
    {
        other.fd = -1;
    }

    CephFaasmSocket& operator=(CephFaasmSocket&& other) noexcept
    {
        if (this != &other) {
            closeFd();
            type = other.type;
            bindAddr = std::move(other.bindAddr);
            fd = other.fd;
            other.fd = -1;
        }
        return *this;
    }

    ~CephFaasmSocket() { closeFd(); }

    int getFd() const { return fd; }

    void setBlocking(bool blocking)
    {
        int flags = ::fcntl(fd, F_GETFL);
        if (flags < 0) {
            throw NdpSocketError(
              errnoMessage("Couldn't get ndp connection flags"));
        }
        flags = blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK);
        if (::fcntl(fd, F_SETFL, flags) < 0) {
            throw NdpSocketError(
              errnoMessage("Couldn't set O_NONBLOCK on the ndp connection"));
        }
    }

    // True if the socket is ready for `event`, or has hung up or failed (the
    // next IO call will then report why). False if the timeout expired.
    bool pollFor(short event, int timeoutMs) const
    {
        const auto deadline = Clock::now() + std::chrono::milliseconds(
                                               timeoutMs < 0 ? 0 : timeoutMs);
        return waitUntil(event, deadline);
    }

    // Accepts one pending connection, or returns nullopt if there is none.
    // Throws NdpSocketError for real failures such as running out of fds.
    std::optional<CephFaasmSocket> tryAccept() const
    {
        while (true) {
            int result =
              ::accept4(fd, nullptr, nullptr, SOCK_CLOEXEC | SOCK_NONBLOCK);
            if (result >= 0) {
                return CephFaasmSocket(result, bindAddr);
            }
            switch (errno) {
                case EINTR:
                case ECONNABORTED:
                    continue;
                case EAGAIN:
#if EAGAIN != EWOULDBLOCK
                case EWOULDBLOCK:
#endif
                    return std::nullopt;
                default:
                    throw NdpSocketError(
                      errnoMessage("Couldn't accept a cephcomm connection"));
            }
        }
    }

    void rawSendBytes(const uint8_t* data,
                      size_t dataSize,
                      Clock::time_point deadline) const
    {
        while (dataSize > 0) {
            ssize_t result = ::send(fd, data, dataSize, MSG_NOSIGNAL);
            if (result > 0) {
                data += result;
                dataSize -= size_t(result);
                continue;
            }
            if (result < 0 && errno == EINTR) {
                continue;
            }
            if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                if (!waitUntil(POLLOUT, deadline)) {
                    throw NdpSocketError(
                      "Timed out sending cephcomm data (peer not reading)");
                }
                continue;
            }
            throw NdpSocketError(errnoMessage("Couldn't send cephcomm data"));
        }
    }

    void rawRecvBytes(uint8_t* data,
                      size_t dataSize,
                      Clock::time_point deadline) const
    {
        while (dataSize > 0) {
            ssize_t result = ::recv(fd, data, dataSize, 0);
            if (result > 0) {
                data += result;
                dataSize -= size_t(result);
                continue;
            }
            if (result == 0) {
                throw NdpSocketError(
                  "Couldn't recv cephcomm data: connection closed by peer");
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (!waitUntil(POLLIN, deadline)) {
                    throw NdpSocketError("Timed out receiving cephcomm data");
                }
                continue;
            }
            throw NdpSocketError(errnoMessage("Couldn't recv cephcomm data"));
        }
    }

    void sendError(int timeoutMs = NDP_DEFAULT_IO_TIMEOUT_MS) const
    {
        uint64_t netSize = ::htole64(NDP_ERROR_MARKER);
        rawSendBytes(reinterpret_cast<const uint8_t*>(&netSize),
                     sizeof(netSize),
                     deadlineIn(timeoutMs));
    }

    // Sends the length header and payload as one framed message.
    void sendMessage(const uint8_t* msgData,
                     size_t msgSize,
                     int timeoutMs = NDP_DEFAULT_IO_TIMEOUT_MS) const
    {
        const auto deadline = deadlineIn(timeoutMs);
        uint64_t netSize = ::htole64(uint64_t(msgSize));
        rawSendBytes(reinterpret_cast<const uint8_t*>(&netSize),
                     sizeof(netSize),
                     deadline);
        rawSendBytes(msgData, msgSize, deadline);
    }

    // Receives one framed message. Throws NdpPeerError if the peer sent the
    // error marker, NdpSocketError on any IO or framing problem.
    std::vector<uint8_t> recvMessageVector(
      int timeoutMs = NDP_DEFAULT_IO_TIMEOUT_MS) const
    {
        const auto deadline = deadlineIn(timeoutMs);
        uint64_t netSize = 0;
        rawRecvBytes(
          reinterpret_cast<uint8_t*>(&netSize), sizeof(netSize), deadline);
        const uint64_t sz = ::le64toh(netSize);
        if (sz == NDP_ERROR_MARKER) {
            throw NdpPeerError("Peer reported an error for this operation");
        }
        if (sz > NDP_MAX_MESSAGE_SIZE) {
            throw NdpSocketError("Invalid cephcomm message size " +
                                 std::to_string(sz));
        }
        std::vector<uint8_t> output(static_cast<size_t>(sz));
        rawRecvBytes(output.data(), output.size(), deadline);
        return output;
    }

  private:
    // Accepted connection constructor
    CephFaasmSocket(int fd, std::string bindAddr)
      : type(SocketType::connect)
      , bindAddr(std::move(bindAddr))
      , fd(fd)
    {
    }

    static Clock::time_point deadlineIn(int timeoutMs)
    {
        return Clock::now() +
               std::chrono::milliseconds(timeoutMs < 0 ? 0 : timeoutMs);
    }

    static std::string errnoMessage(const char* what)
    {
        int ec = errno;
        return std::string(what) + ": " + std::strerror(ec);
    }

    void closeFd()
    {
        if (fd >= 0) {
            ::shutdown(fd, SHUT_RDWR);
            ::close(fd);
            fd = -1;
        }
    }

    // See pollFor
    bool waitUntil(short event, Clock::time_point deadline) const
    {
        while (true) {
            auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                          deadline - Clock::now())
                          .count();
            int timeoutMs =
              left <= 0 ? 0 : (left > INT_MAX ? INT_MAX : int(left));
            pollfd pfd = {};
            pfd.fd = fd;
            pfd.events = event;
            int ready = ::poll(&pfd, 1, timeoutMs);
            if (ready > 0) {
                if (pfd.revents & POLLNVAL) {
                    throw NdpSocketError("Ceph-faasm socket is not open");
                }
                return true;
            }
            if (ready == 0) {
                return false;
            }
            if (errno != EINTR && errno != EAGAIN) {
                throw NdpSocketError(
                  errnoMessage("Ceph-faasm socket poll error"));
            }
        }
    }

    void connectWithRetry(const sockaddr_un& addr, int connectTimeoutMs)
    {
        // Retry briefly so a runtime that is starting up, or has a full
        // accept backlog, gets a chance. This runs inside an OSD op thread,
        // so the total wait stays short.
        const auto deadline = deadlineIn(connectTimeoutMs);
        int backoffMs = 5;
        std::string lastError;
        while (true) {
            fd =
              ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
            if (fd < 0) {
                throw NdpSocketError(errnoMessage(
                  "Couldn't create an fd for a ceph-faasm socket"));
            }
            if (::connect(fd,
                          reinterpret_cast<const sockaddr*>(&addr),
                          sizeof(addr)) == 0) {
                return;
            }
            int ec = errno;
            if (ec == EINPROGRESS) {
                if (waitUntil(POLLOUT, deadline)) {
                    int soError = 0;
                    socklen_t len = sizeof(soError);
                    ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &soError, &len);
                    if (soError == 0) {
                        return;
                    }
                    ec = soError;
                } else {
                    ec = ETIMEDOUT;
                }
            }
            closeFd();
            lastError = std::strerror(ec);
            const bool retryable = ec == ENOENT || ec == ECONNREFUSED ||
                                   ec == EAGAIN || ec == ETIMEDOUT;
            if (!retryable) {
                throw NdpSocketError(
                  "Couldn't connect to the Faasm storage runtime: " +
                  lastError);
            }
            if (Clock::now() + std::chrono::milliseconds(backoffMs) >=
                deadline) {
                throw NdpRuntimeUnavailable(
                  "Faasm storage runtime not available: " + lastError);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(backoffMs));
            backoffMs = backoffMs * 2 > 200 ? 200 : backoffMs * 2;
        }
    }

    SocketType type = SocketType::connect;
    std::string bindAddr;
    int fd = -1;
};

template<class FBType>
void verifyFlatbuf(const uint8_t* dataPtr, size_t dataSz)
{
    flatbuffers::Verifier verifier(dataPtr, dataSz);
    if (!verifier.VerifyBuffer<FBType>(nullptr)) {
        throw std::runtime_error("Invalid Flatbuffer encountered!");
    }
}

template<class FBType>
void verifyFlatbuf(const std::vector<uint8_t>& data)
{
    return verifyFlatbuf<FBType>(data.data(), data.size());
}

}
