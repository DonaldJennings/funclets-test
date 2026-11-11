#include <asm-generic/errno.h>
#include <flatbuffers/buffer.h>
#include <rados/buffer_fwd.h>
#include <sys/poll.h>
#include <unistd.h>
#define FMT_ENFORCE_COMPILE_STRING

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <flatbuffers/flatbuffer_builder.h>
#include <flatbuffers/verifier.h>
#include <fmt/compile.h>
#include <fmt/core.h>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <rados/objclass.h>
#include <stdexcept>

#include "cephclass/cephcomm.h"
#include "cephcomm_generated.h"

#include "../../faabric/include/faabric/util/logging.h"

CLS_VER(1, 0)
CLS_NAME(faasm)

cls_handle_t h_class;
cls_method_handle_t h_maybe_exec_wasm_ro;
cls_method_handle_t h_maybe_exec_wasm_rw;
cls_method_handle_t h_maybe_exec_wasm_wo;
std::string myHostname;

int maybe_exec_wasm_ro(cls_method_context_t hctx,
                       ceph::buffer::list* in,
                       ceph::buffer::list* out);

int maybe_exec_wasm_rw(cls_method_context_t hctx,
                       ceph::buffer::list* in,
                       ceph::buffer::list* out);

int maybe_exec_wasm_wo(cls_method_context_t hctx,
                       ceph::buffer::list* in,
                       ceph::buffer::list* out);

CLS_INIT(faasm)
{
    CLS_LOG(0, "Loading Faasm-NDP Ceph interface class");

    cls_register("faasm", &h_class);

    // TODO: Determine what PROMOTE does
    cls_register_cxx_method(h_class,
                            "maybe_exec_wasm_ro",
                            CLS_METHOD_RD,
                            maybe_exec_wasm_ro,
                            &h_maybe_exec_wasm_ro);
    cls_register_cxx_method(h_class,
                            "maybe_exec_wasm_rw",
                            CLS_METHOD_RD | CLS_METHOD_WR,
                            maybe_exec_wasm_rw,
                            &h_maybe_exec_wasm_rw);
    cls_register_cxx_method(h_class,
                            "maybe_exec_wasm_wo",
                            CLS_METHOD_WR,
                            maybe_exec_wasm_wo,
                            &h_maybe_exec_wasm_wo);

    char hostnameBuf[256] = { 0 };
    ::gethostname(hostnameBuf, sizeof(hostnameBuf));
    myHostname =
      std::string(hostnameBuf, ::strnlen(hostnameBuf, sizeof(hostnameBuf)));
}

namespace faasm {

namespace fbs = flatbuffers;

std::vector<uint8_t> cephBufferToVecU8(const ceph::buffer::list& buflist)
{
    const size_t totalSize = buflist.length();
    std::vector<uint8_t> output(totalSize);
    auto outIt = output.begin();
    for (const auto& buffer : buflist.buffers()) {
        outIt = std::copy_n(buffer.c_str(), buffer.length(), outIt);
    }
    return output;
}

// The OSD holds the PG lock while this runs, and its op thread heartbeat
// (osd_op_thread_timeout, 15 s by default) is not reset by object classes, so
// every wait here must be bounded.
constexpr int INITIAL_RESPONSE_TIMEOUT_MS = 5000;
constexpr int STORAGE_MESSAGE_TIMEOUT_MS = 30000;
// The compute node's output buffer for the response is 1 KiB
constexpr size_t MAX_ERROR_MSG_LEN = 512;

std::vector<uint8_t> buildNdpResponse(uint64_t callId,
                                      ndpmsg::NdpResult result,
                                      std::string errorMsg)
{
    if (errorMsg.size() > MAX_ERROR_MSG_LEN) {
        errorMsg.resize(MAX_ERROR_MSG_LEN);
    }
    flatbuffers::FlatBufferBuilder builder(256);
    auto errorField = builder.CreateString(errorMsg);
    ndpmsg::NdpResponseBuilder respBuilder(builder);
    respBuilder.add_call_id(callId);
    respBuilder.add_result(result);
    respBuilder.add_error_msg(errorField);
    builder.Finish(respBuilder.Finish());
    return { builder.GetBufferPointer(),
             builder.GetBufferPointer() + builder.GetSize() };
}

int maybe_exec_wasm(cls_method_context_t hctx,
                    ceph::buffer::list* inBuffers,
                    ceph::buffer::list* outBuffers,
                    bool readAllowed,
                    bool writeAllowed)
{
    if (inBuffers == nullptr || outBuffers == nullptr ||
        !(readAllowed || writeAllowed)) {
        SPDLOG_ERROR("Invalid arguments to maybe_exec_wasm");
        return -EINVAL;
    }
    SPDLOG_DEBUG(fmt::format(FMT_STRING("maybe_exec_wasm called {} {} {}"),
                             inBuffers->length(),
                             readAllowed,
                             writeAllowed));

    uint64_t callId = 0;
    ndpmsg::NdpResult result = ndpmsg::NdpResult_Ok;
    std::string errorMsg;

    try {
        CLS_LOG(5,
                "Received NDP call request of size %d bytes",
                (int)inBuffers->length());
        auto input = cephBufferToVecU8(*inBuffers);

        verifyFlatbuf<ndpmsg::NdpRequest>(input);
        const auto* req = flatbuffers::GetRoot<ndpmsg::NdpRequest>(input.data());
        callId = req->call_id();
        SPDLOG_DEBUG(fmt::format(FMT_STRING("Received call id {}"), callId));

        // Connect to the Faasm runtime. If it isn't running, let the compute
        // node run the call itself rather than failing it.
        std::optional<CephFaasmSocket> runtimeHolder;
        try {
            runtimeHolder.emplace(SocketType::connect);
        } catch (const NdpRuntimeUnavailable& e) {
            SPDLOG_WARN(fmt::format(
              FMT_STRING("NDP call {}: {}, asking compute node to process "
                         "locally"),
              callId,
              e.what()));
            auto resp =
              buildNdpResponse(callId, ndpmsg::NdpResult_ProcessLocally, "");
            outBuffers->append(reinterpret_cast<const char*>(resp.data()),
                               resp.size());
            return 0;
        }
        CephFaasmSocket& runtime = *runtimeHolder;

        // Forward the request from Storage Ceph to Storage Faasm
        CLS_LOG(5,
                "Forwarding call request %llu to the Faasm runtime",
                static_cast<unsigned long long>(callId));
        fbs::FlatBufferBuilder fwdBuilder(input.size() + 128);
        {
            auto reqOffset = fwdBuilder.CreateVector(input);
            auto hnOffset = fwdBuilder.CreateString(myHostname);
            auto ndpOffset =
              ndpmsg::CreateCephNdpRequest(fwdBuilder, reqOffset, hnOffset);
            fwdBuilder.Finish(ndpOffset);
        }
        runtime.sendMessage(fwdBuilder.GetBufferPointer(),
                            fwdBuilder.GetSize(),
                            INITIAL_RESPONSE_TIMEOUT_MS);

        if (!runtime.pollFor(POLLIN, INITIAL_RESPONSE_TIMEOUT_MS)) {
            throw std::runtime_error(
              "Timed out waiting for the Faasm runtime to accept the call");
        }
        const auto initialRuntimeResponseData =
          runtime.recvMessageVector(INITIAL_RESPONSE_TIMEOUT_MS);
        verifyFlatbuf<ndpmsg::NdpResponse>(initialRuntimeResponseData);
        const auto* initialRuntimeResponse =
          flatbuffers::GetRoot<ndpmsg::NdpResponse>(
            initialRuntimeResponseData.data());

        if (initialRuntimeResponse->call_id() != callId) {
            throw std::runtime_error("Mismatched call ids!");
        }
        if (initialRuntimeResponse->result() != ndpmsg::NdpResult_Ok) {
            // ProcessLocally or Error: forward it to the compute node as is
            SPDLOG_DEBUG("Faasm runtime did not accept the call");
            outBuffers->append(
              reinterpret_cast<const char*>(initialRuntimeResponseData.data()),
              initialRuntimeResponseData.size());
            return 0;
        }

        // Serve the funclet's reads and writes until it sends NdpEnd
        bool running = true;
        ceph::buffer::list readBufferList;
        while (running) {
            readBufferList.clear();

            if (!runtime.pollFor(POLLIN, STORAGE_MESSAGE_TIMEOUT_MS)) {
                throw std::runtime_error(
                  "Timed out waiting for Faasm-storage messages");
            }

            const auto msgData =
              runtime.recvMessageVector(STORAGE_MESSAGE_TIMEOUT_MS);
            verifyFlatbuf<ndpmsg::StorageMessage>(msgData);
            const auto* topMsg =
              flatbuffers::GetRoot<ndpmsg::StorageMessage>(msgData.data());
            if (topMsg->call_id() != callId) {
                throw std::runtime_error(
                  "Mismatched call ids in storage message!");
            }
            switch (topMsg->message_type()) {
                case ndpmsg::TypedStorageMessage_NdpEnd: {
                    CLS_LOG(5, "Received NDP end");
                    const auto* msg = topMsg->message_as_NdpEnd();
                    if (msg->result() != ndpmsg::NdpResult_Ok) {
                        result = ndpmsg::NdpResult_Error;
                        errorMsg = "Faasm storage runtime failed the call";
                        if (msg->error_msg() != nullptr &&
                            msg->error_msg()->size() > 0) {
                            errorMsg =
                              std::string(reinterpret_cast<const char*>(
                                            msg->error_msg()->data()),
                                          msg->error_msg()->size());
                        }
                    }
                    running = false;
                    break;
                }
                case ndpmsg::TypedStorageMessage_NdpRead: {
                    CLS_LOG(5, "Received NDP read");
                    const auto* msg = topMsg->message_as_NdpRead();
                    if (!readAllowed) {
                        runtime.sendError();
                        throw std::runtime_error(
                          "NDP read in a write-only object class method");
                    }
                    if (msg->offset() > uint64_t(INT32_MAX)) {
                        runtime.sendError();
                        throw std::runtime_error("NDP read offset too large");
                    }
                    const int readLen =
                      int(std::min<uint64_t>(msg->upto_length(), INT32_MAX));
                    int ec = cls_cxx_read(
                      hctx, int(msg->offset()), readLen, &readBufferList);
                    if (ec < 0) {
                        runtime.sendError();
                        throw std::runtime_error(
                          std::string("Error in a read operation: ") +
                          strerror(-ec));
                    }
                    runtime.sendMessage(
                      reinterpret_cast<const uint8_t*>(readBufferList.c_str()),
                      readBufferList.length());
                    break;
                }
                case ndpmsg::TypedStorageMessage_NdpWrite: {
                    CLS_LOG(5, "Received NDP write");
                    const auto* msg = topMsg->message_as_NdpWrite();
                    if (!writeAllowed) {
                        // Writing from a method registered without
                        // CLS_METHOD_WR trips
                        // ceph_assert(op->may_write() || op->may_cache())
                        // in PrimaryLogPG::execute_ctx and aborts the OSD.
                        runtime.sendError();
                        throw std::runtime_error(
                          "NDP write to the call's object in a read-only "
                          "call: use maybe_exec_wasm_rw for functions that "
                          "write to their input object");
                    }
                    if (msg->data() == nullptr ||
                        msg->offset() > uint64_t(INT32_MAX) ||
                        msg->data()->size() > uint32_t(INT32_MAX)) {
                        runtime.sendError();
                        throw std::runtime_error("Invalid NDP write");
                    }
                    // Const cast safety: we do not modify the data in the
                    // buffer.
                    auto writeBufferList = ceph::bufferlist::static_from_mem(
                      const_cast<char*>(
                        reinterpret_cast<const char*>(msg->data()->data())),
                      msg->data()->size());
                    int ec = cls_cxx_write(hctx,
                                           int(msg->offset()),
                                           int(msg->data()->size()),
                                           &writeBufferList);
                    if (ec < 0) {
                        runtime.sendError();
                        throw std::runtime_error(
                          std::string("Error in a write operation: ") +
                          strerror(-ec));
                    }
                    uint8_t OK_MSG[2] = { 'o', 'k' };
                    runtime.sendMessage(OK_MSG, sizeof(OK_MSG));
                    break;
                }
                default: {
                    runtime.sendError();
                    throw std::runtime_error("Invalid storage message type");
                }
            }
        }
    } catch (const std::exception& e) {
        result = ndpmsg::NdpResult_Error;
        errorMsg = fmt::format(
          FMT_STRING("Exception caught in maybe_exec_wasm call {}: {}"),
          callId,
          e.what());
        CLS_LOG(0, "%s", errorMsg.c_str());
        SPDLOG_ERROR(errorMsg.c_str());
    } catch (...) {
        result = ndpmsg::NdpResult_Error;
        errorMsg = fmt::format(
          FMT_STRING(
            "Unknown exception type caught in maybe_exec_wasm call {}"),
          callId);
        CLS_LOG(0, "%s", errorMsg.c_str());
        SPDLOG_ERROR(errorMsg.c_str());
    }

    // Construct response to Compute Faasm runtime
    auto resp = buildNdpResponse(callId, result, errorMsg);
    outBuffers->append(reinterpret_cast<const char*>(resp.data()), resp.size());
    return 0;
}
}

int maybe_exec_wasm_ro(cls_method_context_t hctx,
                       ceph::buffer::list* in,
                       ceph::buffer::list* out)
{
    return faasm::maybe_exec_wasm(hctx, in, out, true, false);
}

int maybe_exec_wasm_rw(cls_method_context_t hctx,
                       ceph::buffer::list* in,
                       ceph::buffer::list* out)
{
    return faasm::maybe_exec_wasm(hctx, in, out, true, true);
}

int maybe_exec_wasm_wo(cls_method_context_t hctx,
                       ceph::buffer::list* in,
                       ceph::buffer::list* out)
{
    return faasm::maybe_exec_wasm(hctx, in, out, false, true);
}
