#pragma once
#include <faabric/scheduler/Scheduler.h>
#include <string>

namespace faabric::util {
faabric::Message messageFactory(const std::string& user,
                                const std::string& function);
}
