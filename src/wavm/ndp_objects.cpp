#include "WAVMWasmModule.h"
#include "cephcomm_generated.h"
#include "faabric/scheduler/Scheduler.h"
#include "syscalls.h"

#include <exception>
#include <faabric/scheduler/ExecutorContext.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/util/bytes.h>
#include <faabric/util/delta.h>
#include <faabric/util/files.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/timing.h>
#include <memory>
#include <mutex>
#include <optional>

#include <stdexcept>
#include <storage/S3Wrapper.h>
#include <wasm/WasmExecutionContext.h>
#include <wasm/chaining.h>
#include <wasm/ndp.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <span>
#include <string_view>

#include <WAVM/IR/IR.h>
#include <WAVM/Inline/FloatComponents.h>
#include <WAVM/Runtime/Intrinsics.h>
#include <WAVM/Runtime/Runtime.h>

using namespace WAVM;

namespace wasm {

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
                               "__faasmndp_put",
                               I32,
                               __faasmndp_put,
                               I32 keyPtr,
                               I32 keyLen,
                               I32 dataPtr,
                               I32 dataLen)
{
    ZoneScopedN("__faasmndp_put");
    static storage::S3Wrapper s3w;
    if (keyPtr <= 0 || keyLen <= 0 || dataPtr <= 0 || dataLen <= 0) {
        return -1;
    }

    SPDLOG_DEBUG("[ndp_objects::__faasmndp_put] Fetching system config and retrieving executing call");
    const faabric::util::SystemConfig& config = faabric::util::getSystemConfig();
    faabric::Message* executingCall = &faabric::scheduler::ExecutorContext::get()->getMsg();
    
    SPDLOG_DEBUG("[ndp_objects::__faasmndp_put] Fetching executing WASM module and memory pointer");
    WAVMWasmModule* module_ = getExecutingWAVMModule();
    Runtime::Memory* memoryPtr = module_->defaultMemory;
    
    SPDLOG_DEBUG("[ndp_objects::__faasmndp_put] Fetching key and data pointers");
    U8* key = Runtime::memoryArrayPtr<U8>(memoryPtr, (Uptr)keyPtr, (Uptr)keyLen);
    std::string_view keyStr(reinterpret_cast<char*>(key), keyLen);
    U8* data = Runtime::memoryArrayPtr<U8>(memoryPtr, (Uptr)dataPtr, (Uptr)dataLen);

    SPDLOG_DEBUG("S - __faasmndp_put - {} {} {} {}", keyPtr, keyLen, dataPtr, dataLen);

    try {
        using namespace faasm;
        if (config.isStorageNode &&
            keyStr == executingCall->ndpcallobjectname()) {
            flatbuffers::FlatBufferBuilder builder(256);
            auto dataField = builder.CreateVector(data, dataLen);
            ndpmsg::NdpWriteBuilder reqBuilder(builder);
            reqBuilder.add_offset(0);
            reqBuilder.add_data(dataField);
            auto reqOffset = reqBuilder.Finish();
            ndpmsg::StorageMessageBuilder msgBuilder(builder);
            msgBuilder.add_call_id(executingCall->id());
            msgBuilder.add_message_type(ndpmsg::TypedStorageMessage_NdpWrite);
            msgBuilder.add_message(reqOffset.Union());
            auto msgOffset = msgBuilder.Finish();
            builder.Finish(msgOffset);

            SPDLOG_DEBUG("[ndp_objects::__faasmndp_put] Sending NDP write request");
            auto response = ndpStorageRoundTrip(
              executingCall->id(),
              std::span(builder.GetBufferPointer(), builder.GetSize()));
            if (response.size() != 2 || response[0] != 'o' ||
                response[1] != 'k') {
                throw std::runtime_error(
                  "Unexpected response from CEPH storage operation");
            }
        } else {
            if (config.isStorageNode) {
                SPDLOG_WARN("Running a storage operation on non-hinted object. "
                            "id={}, hinted=`{}`, key=`{}`",
                            executingCall->id(),
                            executingCall->ndpcallobjectname(),
                            keyStr);
            }
            s3w.addKeyBytes(module_->getBoundUser(),
                            std::string(keyStr),
                            std::span(data, dataLen));
        }
    } catch (const std::exception& err) {
        SPDLOG_ERROR("__faasmndp_put error: {}", err.what());
        return -1;
    }

    return 0;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
                               "__faasmndp_unmap",
                               I32,
                               __faasmndp_unmap,
                               I32 offset,
                               I32 length)
{
    SPDLOG_INFO("Unmapping at {} : {} bytes", offset, length);
    ZoneScopedN("__faasmndp_unmap");

    WAVMWasmModule* module_ = static_cast<WAVMWasmModule*>(
      getCurrentWasmExecutionContext()->executingModule);
    module_->unmapMemory(offset, length);

    return 0;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
                               "__faasmndp_getMmap",
                               I32,
                               __faasmndp_getMmap,
                               I32 keyPtr,
                               I32 keyLen,
                               I64 offset,
                               I64 maxRequestedLen,
                               I32 outDataLenPtr)
{
    static storage::S3Wrapper s3w;
    ZoneScopedN("__faasmndp_getMmap");
    if (keyPtr <= 0 || keyLen <= 0 || maxRequestedLen < 0 ||
        maxRequestedLen > INT32_MAX || offset < 0) {
        return 0;
    }

    const faabric::util::SystemConfig& config =
      faabric::util::getSystemConfig();

    faabric::Message* executingCall =
      &faabric::scheduler::ExecutorContext::get()->getMsg();
    WAVMWasmModule* module_ = static_cast<WAVMWasmModule*>(
      getCurrentWasmExecutionContext()->executingModule);
    Runtime::Memory* memoryPtr = module_->defaultMemory;
    U8* key =
      Runtime::memoryArrayPtr<U8>(memoryPtr, (Uptr)keyPtr, (Uptr)keyLen);
    std::string_view keyStr(reinterpret_cast<char*>(key), keyLen);
    U32* outDataLen = &Runtime::memoryRef<U32>(memoryPtr, (Uptr)outDataLenPtr);
    *outDataLen = 0;

    SPDLOG_DEBUG("S - __faasmndp_getMmap - {} {} key=`{}` {:x} {}",
                 keyPtr,
                 keyLen,
                 keyStr,
                 maxRequestedLen,
                 outDataLenPtr);

    faabric::Message get_result;
    std::unique_ptr<WasmExecutionContext> wec;

    U32 oldPagesEnd = 0;
    ssize_t allocLen = 0;
    char* bufferStart = nullptr;
    ssize_t actualLength = 0;

    auto setBufferLength = [&](ssize_t len) {
        oldPagesEnd = module_->mmapMemory(len);
        allocLen = len;
        bufferStart =
          Runtime::memoryArrayPtr<char>(memoryPtr, oldPagesEnd, len);
    };

    try {
        using namespace faasm;
        if (config.isStorageNode &&
            keyStr == executingCall->ndpcallobjectname()) {
            flatbuffers::FlatBufferBuilder builder(256);
            ndpmsg::NdpReadBuilder reqBuilder(builder);
            reqBuilder.add_offset(offset);
            reqBuilder.add_upto_length(maxRequestedLen);
            auto reqOffset = reqBuilder.Finish();
            ndpmsg::StorageMessageBuilder msgBuilder(builder);
            msgBuilder.add_call_id(executingCall->id());
            msgBuilder.add_message_type(ndpmsg::TypedStorageMessage_NdpRead);
            msgBuilder.add_message(reqOffset.Union());
            auto msgOffset = msgBuilder.Finish();
            builder.Finish(msgOffset);

            const auto response = ndpStorageRoundTrip(
              executingCall->id(),
              std::span(builder.GetBufferPointer(), builder.GetSize()));
            SPDLOG_DEBUG("NDP Read response of size {}", response.size());
            setBufferLength(response.size());
            actualLength = response.size();
            std::memcpy(bufferStart, response.data(), response.size());
        } else {
            if (config.isStorageNode) {
                SPDLOG_WARN("Running a storage operation on non-hinted object. "
                            "id={}, hinted=`{}`, key=`{}`",
                            executingCall->id(),
                            executingCall->ndpcallobjectname(),
                            keyStr);
            }

            actualLength = s3w.getKeyPartIntoBuf(module_->getBoundUser(),
                                                 std::string(keyStr),
                                                 offset,
                                                 maxRequestedLen,
                                                 setBufferLength,
                                                 [&]() { return bufferStart; });
        }
    } catch (const std::exception& err) {
        SPDLOG_ERROR("__faasmndp_getMmap error: {}", err.what());
        return -1;
    }

    *outDataLen = actualLength;
    if (bufferStart == nullptr || actualLength == 0) {
        return 0;
    }
    module_->snapshotExcludedPtrLens.emplace_back(oldPagesEnd, allocLen);
    SPDLOG_INFO("mmap buffer at {} : {} bytes", oldPagesEnd, allocLen);

    return oldPagesEnd;
}

namespace {
// Lends the calling module to the NDP delta handler for as long as the
// offloaded call is in progress. The handler runs on a FunctionCallServer
// thread when the storage node asks for the memory delta. Once this guard is
// destroyed (on every exit path), a late request can no longer reach the
// module, and destruction waits for a request that is already being served.
class NdpDeltaHandlerGuard
{
  public:
    NdpDeltaHandlerGuard(int ndpCallId, WAVMWasmModule* module, int callId)
      : ndpCallId(ndpCallId)
      , state(std::make_shared<State>())
    {
        state->module = module;
        faabric::scheduler::FunctionCallServer::registerNdpDeltaHandler(
          ndpCallId, [state = state, callId]() -> std::vector<uint8_t> {
              // An exception here would escape the FunctionCallServer thread
              try {
                  std::lock_guard lock(state->mx);
                  if (state->module == nullptr) {
                      SPDLOG_ERROR("{} - NDP delta requested after the call "
                                   "ended",
                                   callId);
                      return {};
                  }
                  std::shared_ptr<faabric::state::StateKeyValue>
                    zygoteSnapshotKV = state->module->getZygoteSnapshot();
                  const std::span<const uint8_t> zygoteSnapshot{
                      zygoteSnapshotKV->get(), zygoteSnapshotKV->size()
                  };
                  std::vector<uint8_t> zygoteDelta =
                    state->module->deltaSnapshot(zygoteSnapshot);
                  SPDLOG_INFO("{} - NDP sending snapshot of {} bytes",
                              callId,
                              zygoteDelta.size());
                  return zygoteDelta;
              } catch (const std::exception& e) {
                  SPDLOG_ERROR(
                    "{} - couldn't build NDP delta: {}", callId, e.what());
                  return {};
              }
          });
    }

    NdpDeltaHandlerGuard(const NdpDeltaHandlerGuard&) = delete;
    NdpDeltaHandlerGuard& operator=(const NdpDeltaHandlerGuard&) = delete;

    ~NdpDeltaHandlerGuard()
    {
        faabric::scheduler::FunctionCallServer::removeNdpDeltaHandler(
          ndpCallId);
        std::lock_guard lock(state->mx);
        state->module = nullptr;
    }

  private:
    struct State
    {
        std::mutex mx;
        WAVMWasmModule* module = nullptr;
    };

    int ndpCallId;
    std::shared_ptr<State> state;
};
}

I32 storageCallAndAwaitImpl(I32 keyPtr,
                            I32 keyLen,
                            I32 wasmFuncPtr,
                            I32 pyFuncNamePtr,
                            std::span<I32> extraArgs)
{
    ZoneScopedN("storageCallAndAwaitImpl");
    SPDLOG_DEBUG(" ========= EXECUTING STORAGE CALL AND AWAIT IMPL =========");
    SPDLOG_DEBUG("[ndp_objects] storageCallAndAwaitImpl entered");
    static storage::S3Wrapper s3w;
    if (keyPtr <= 0 || keyLen <= 0) { return 0; }
    const faabric::util::SystemConfig& config = faabric::util::getSystemConfig();

    // Extract python function name
    const bool isPython = pyFuncNamePtr != 0;
    const std::string pyFuncName = isPython ? getStringFromWasm(pyFuncNamePtr) : "";
    SPDLOG_DEBUG("S - storageCallAndAwaitImpl - WASM Func ptr={} Python Func name={} #{}",
                 wasmFuncPtr,
                 pyFuncName,
                 extraArgs.size());

    faabric::Message* call = &faabric::scheduler::ExecutorContext::get()->getMsg();
    WAVMWasmModule* thisModule = static_cast<WAVMWasmModule*>(
        getCurrentWasmExecutionContext()->executingModule); // ptr to current WASM Module

    Runtime::Memory* memoryPtr = thisModule->defaultMemory;
    U8* key = Runtime::memoryArrayPtr<U8>(memoryPtr, (Uptr)keyPtr, (Uptr)keyLen);
    std::string keyStr(reinterpret_cast<char*>(key), keyLen);

    // Validate function signature
    if (!isPython) {
        SPDLOG_DEBUG("[ndp_objects::storageCallAndAwaitImpl] Extracting function signature for C++ program");
        auto* funcInstance = thisModule->getFunctionFromPtr(wasmFuncPtr);
        auto funcType = Runtime::getFunctionType(funcInstance);

        // If extracted function signature does not match the actual function signature
        if (funcType.results().size() != 1 || funcType.params().size() != extraArgs.size())
        {
            throw std::invalid_argument("Wrong function signature for storageCallAndAwait");
        }

        // Ensure function argument are i32's
        for (const auto& param : funcType.params())
        {
            if (param != IR::ValueType::i32)
            {
                throw std::invalid_argument("Function argument not i32");
            }
        }
    }

    bool callLocally = true; // flag set to true when function should be called on current node, false when offloaded
    if (!call->forbidndp() && !config.isStorageNode) {
        SPDLOG_INFO("[ndp_objects::storageCallAndAwaitImpl] NDP Call detected, offloading funclets to the relevant storage node");
        using namespace faasm;
        callLocally = false;

        std::vector<int32_t> wasmGlobals = thisModule->getGlobals();

        const int ndpCallId = faabric::util::generateGid() & 0xFFFF'FFFF;
        // The storage node fetches the memory delta while the rados call
        // below is in flight. Reset straight after it returns, and on every
        // exit path from this block.
        std::optional<NdpDeltaHandlerGuard> deltaHandler;
        deltaHandler.emplace(ndpCallId, thisModule, call->id());
        faabric::scheduler::getScheduler().addLocalResultSlot(ndpCallId);

        // begin ceph aio
        flatbuffers::FlatBufferBuilder builder(256);
        {
            auto fBucket   = builder.CreateString(call->user());
            auto fFunction = builder.CreateString(call->function());
            auto fKey      = builder.CreateString(keyStr);
            auto fObjInfo  = ndpmsg::CreateObjectInfo(builder, fBucket, fKey);

            auto fPyPtr    = builder.CreateString(pyFuncName);
            auto fGlobals  = builder.CreateVector(wasmGlobals);
            auto fArgs     = builder.CreateVector(extraArgs.data(), extraArgs.size());
            auto fWasmInfo = ndpmsg::CreateWasmInfo(builder,
                                                    fBucket,
                                                    fFunction,
                                                    wasmFuncPtr,
                                                    fPyPtr,
                                                    fGlobals,
                                                    fArgs);
            
            SPDLOG_DEBUG("[ndp_objects] Making NDP request with call id {}", ndpCallId);
            auto fOriginHost = builder.CreateString(config.endpointHost);
            auto ndpRequest  = ndpmsg::CreateNdpRequest(builder, ndpCallId, fWasmInfo, fObjInfo, fOriginHost);
            builder.Finish(ndpRequest);
        }

        SPDLOG_DEBUG("Sent storage request to bucket={}, key={}, function={}, call_id={}, origin_host={}",
                     call->user(),
                     keyStr,
                     call->function(),
                     ndpCallId,
                     config.endpointHost);
                     
        const std::span<const uint8_t> inputSpan(builder.GetBufferPointer(),
                                                 builder.GetSize());

        std::vector<uint8_t> cephOutput(1024);

        // Returns the result of the object class call: the output length, or
        // a negative errno
        int cephEc = s3w.asyncNdpCall(call->user(),
                                      keyStr,
                                      "faasm",
                                      "maybe_exec_wasm_ro",
                                      inputSpan,
                                      cephOutput);
        deltaHandler.reset();

        SPDLOG_DEBUG("asyncNdpCall returned {}", cephEc);

        if (cephEc < 0) { // if error occurred
            std::string cephErrStr = "<? ";
            try {
                verifyFlatbuf<ndpmsg::NdpResponse>(cephOutput);
                const auto* ndpResponse = flatbuffers::GetRoot<ndpmsg::NdpResponse>(cephOutput.data());
                cephErrStr = ndpResponse->error_msg()->str();
            } catch (std::exception& e) {
                cephErrStr = fmt::format(
                  "<? could not decode {} bytes: {}>",
                  cephOutput.size() -
                    std::count(cephOutput.cbegin(), cephOutput.cend(), 0),
                  e.what());
            }
            SPDLOG_ERROR(
              "Ceph NDP call {} for {}/{} failed with error `{}`: {}",
              ndpCallId,
              call->user(),
              call->function(),
              strerror(-cephEc),
              cephErrStr);
            throw std::runtime_error("Ceph NDP call failed with an error.");
        }

        verifyFlatbuf<ndpmsg::NdpResponse>(cephOutput);
        const auto* ndpResponse = flatbuffers::GetRoot<ndpmsg::NdpResponse>(cephOutput.data()); // Construct response based on Ceph output buffer
        if (ndpResponse->call_id() != ndpCallId) 
        {
            throw std::runtime_error("Mismatched call ids in storage message!");
        }

        // State Machine for NDP Response codes
        switch (ndpResponse->result()) {
            case ndpmsg::NdpResult_Ok:
                SPDLOG_INFO("Ceph NDP result for {}/{} returned OK", call->user(), call->function());
                break;
            case ndpmsg::NdpResult_Error: {
                SPDLOG_ERROR("Ceph NDP call {} for {}/{} returned error {}",
                             ndpCallId,
                             call->user(),
                             call->function(),
                             ndpResponse->error_msg()->str());
                throw std::runtime_error("Ceph NDP call returned an error.");
            }
            case ndpmsg::NdpResult_ProcessLocally: {
                SPDLOG_INFO("Ceph NDP call returned ProcessLocally, calling locally");
                callLocally = true;
                break;
            }
            default:
                throw std::runtime_error("Invalid NDP result code");
        }

        if (!callLocally) {
            SPDLOG_DEBUG("[ndp_objects] Awaiting chained NDP call");
            faabric::Message ndpResult = awaitChainedCallMessage(ndpCallId);

            if (ndpResult.returnvalue() != 0) // error occurred
            {
                call->set_outputdata(ndpResult.outputdata());
                SPDLOG_DEBUG("Chained NDP resulted in error code {}", ndpResult.returnvalue());
                return ndpResult.returnvalue();
            }

            {
                // restore globals
                for (int i = 0; i < ndpResult.wasmglobals_size(); i++) {
                    int32_t value = ndpResult.wasmglobals(i);
                    SPDLOG_DEBUG("Restoring global #{}: new {} <- old {}",
                                 i,
                                 value,
                                 wasmGlobals.at(i));
                    thisModule->updateGlobal(i, value);
                }
            }

            SPDLOG_INFO("{} - NDP delta restore from {} bytes", call->id(), ndpResult.outputdata().size());
            // faabric::util::writeBytesToFile(
            //   "/usr/local/faasm/debug_shared_store/debug_delta.bin",
            //   faabric::util::stringToBytes(ndpResult.outputdata()));
            std::span<const uint8_t> memoryDelta =
              std::span(BYTES_CONST(ndpResult.outputdata().data()),
                        ndpResult.outputdata().size());
            thisModule->deltaRestore(memoryDelta);
        }
    }

    // if calling locally
    if (callLocally) {
        ZoneScopedN("call locally");
        SPDLOG_DEBUG("[ndp_objects] Calling locally");
        if (isPython) {
            return -0x12345678;
        }

        // Initialise function with arguments
        auto* funcInstance = thisModule->getFunctionFromPtr(wasmFuncPtr);
        auto funcType = Runtime::getFunctionType(funcInstance);
        std::vector<IR::UntaggedValue> funcArgs;
        funcArgs.reserve(extraArgs.size() + 1);
        for (I32 param : extraArgs) 
        {
            funcArgs.push_back(param);
        }
        funcArgs.push_back(0);
        IR::UntaggedValue result;

        // Invoke function on the current faasm runtime
        try {
            Runtime::invokeFunction(thisModule->executionContext,
                                    funcInstance,
                                    funcType,
                                    funcArgs.data(),
                                    &result);
        } catch (std::exception& e) {
            SPDLOG_ERROR("Caught exception: {}", e.what());
            return -0x12345678;
        }  
        return result.i32;
    }
    SPDLOG_DEBUG(" ========= EXITING storageCallAndAwaitImpl =========");
    return 0;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
                               "__faasmndp_storageCallAndAwait",
                               I32,
                               __faasmndp_storageCallAndAwait,
                               I32 keyPtr,
                               I32 keyLen,
                               I32 wasmFuncPtr)
{
    return storageCallAndAwaitImpl(keyPtr, keyLen, wasmFuncPtr, 0, {});
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
                               "__faasmndp_storageCallAndAwait1",
                               I32,
                               __faasmndp_storageCallAndAwait1,
                               I32 keyPtr,
                               I32 keyLen,
                               I32 wasmFuncPtr,
                               I32 arg1)
{
    std::array args{ arg1 };
    return storageCallAndAwaitImpl(keyPtr, keyLen, wasmFuncPtr, 0, args);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
                               "__faasmndp_storageCallAndAwait2",
                               I32,
                               __faasmndp_storageCallAndAwait2,
                               I32 keyPtr,
                               I32 keyLen,
                               I32 wasmFuncPtr,
                               I32 arg1,
                               I32 arg2)
{
    std::array args{ arg1, arg2 };
    return storageCallAndAwaitImpl(keyPtr, keyLen, wasmFuncPtr, 0, args);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
                               "__faasmndp_storageCallAndAwait_py",
                               I32,
                               __faasmndp_storageCallAndAwait_py,
                               I32 keyPtr,
                               I32 keyLen,
                               I32 namePtr)
{
    return storageCallAndAwaitImpl(keyPtr, keyLen, 0, namePtr, {});
}

void ndpLink() {}

}
