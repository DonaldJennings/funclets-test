#pragma once
// Stand-in for faabric's Scheduler / Message. Only the API surface used by
// src/wasm/ndp_endpoint.cpp is provided. The storage simulator installs an
// executor hook that runs the "funclet" on its own thread, and drops the
// extraData pointer when the task finishes, exactly like
// faabric::scheduler::Executor::threadPoolThread does (task = ExecutorTask()).
#include <cstdint>
#include <functional>
#include <memory>
#include <semaphore>
#include <string>
#include <vector>

#include <faabric/util/config.h>

namespace faabric {

class Message
{
  public:
    struct RepeatedI32
    {
        std::vector<int32_t> v;
        template<class It>
        void Assign(It b, It e)
        {
            v.assign(b, e);
        }
    };

    int32_t id() const { return id_; }
    void set_id(int32_t v) { id_ = v; }
    int32_t appid() const { return appid_; }
    void set_appid(int32_t v) { appid_ = v; }
    const std::string& user() const { return user_; }
    void set_user(const std::string& v) { user_ = v; }
    const std::string& function() const { return function_; }
    void set_function(const std::string& v) { function_ = v; }
    int32_t funcptr() const { return funcptr_; }
    void set_funcptr(int32_t v) { funcptr_ = v; }
    bool ispython() const { return ispython_; }
    void set_ispython(bool v) { ispython_ = v; }
    void set_pythonfunction(const std::string& v) { pythonfunction_ = v; }
    void set_pythonuser(const std::string& v) { pythonuser_ = v; }
    bool isstorage() const { return isstorage_; }
    void set_isstorage(bool v) { isstorage_ = v; }
    void set_isoutputmemorydelta(bool v) { isoutputmemorydelta_ = v; }
    const std::string& directresulthost() const { return directresulthost_; }
    void set_directresulthost(const std::string& v) { directresulthost_ = v; }
    RepeatedI32* mutable_wasmglobals() { return &wasmglobals_; }
    RepeatedI32* mutable_extraarguments() { return &extraarguments_; }
    const std::string& ndpcallobjectname() const { return ndpcallobjectname_; }
    void set_ndpcallobjectname(const std::string& v) { ndpcallobjectname_ = v; }
    bool isasync() const { return isasync_; }

  private:
    int32_t id_ = 0;
    int32_t appid_ = 0;
    std::string user_;
    std::string function_;
    int32_t funcptr_ = 0;
    bool ispython_ = false;
    std::string pythonfunction_;
    std::string pythonuser_;
    bool isstorage_ = false;
    bool isoutputmemorydelta_ = false;
    std::string directresulthost_;
    RepeatedI32 wasmglobals_;
    RepeatedI32 extraarguments_;
    std::string ndpcallobjectname_;
    bool isasync_ = false;
};

class MessageRecord
{};

namespace scheduler {

class Scheduler
{
  public:
    Scheduler();

    void callFunction(faabric::Message& msg,
                      bool forceLocal = false,
                      const MessageRecord& caller = {},
                      std::shared_ptr<void> extraData = nullptr);

    std::counting_semaphore<16384> executionSlotsSemaphore;

    // Installed by the simulator
    std::function<void(faabric::Message, std::shared_ptr<void>)> executeHook;
};

Scheduler& getScheduler();
}
}
