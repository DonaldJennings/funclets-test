#pragma once
// Stand-in for faabric's SystemConfig: only the fields the NDP code reads.
// Defaults and env var names match faabric's donald-ug branch.
#include <cstdlib>
#include <string>

namespace faabric::util {
class SystemConfig
{
  public:
    SystemConfig();

    bool isStorageNode = true;
    double offload_cpu_threshold = 0.8;
    double offload_ram_threshold = 0.8;
    double offload_load_avg_threshold = 0.8;
    int overrideCpuCount = 0;
    int endpointNumThreads = 4;
    std::string endpointHost = "storage-sim";
};

SystemConfig& getSystemConfig();
}
