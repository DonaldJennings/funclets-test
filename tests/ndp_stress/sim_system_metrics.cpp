// Lets a scenario pin the load readings the NDP endpoint sees, so the socket
// protocol can be stressed without the load-balancing policy getting in the
// way. With NDP_SIM_UTILISATION unset, faabric's real implementation is used.
// faabric's system_metrics.cpp is compiled with its functions renamed to
// real_* (see CMakeLists.txt).
#include <faabric/util/system_metrics.h>

#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>

namespace faabric::util {
UtilisationStats real_getSystemUtilisation();
CPUStats real_getCPUUtilisation();
double real_getMemoryUtilisation();
double real_getLoadAverage();

namespace {
std::optional<double> fakeUtilisation()
{
    static std::optional<double> v = []() -> std::optional<double> {
        const char* s = std::getenv("NDP_SIM_UTILISATION");
        if (s == nullptr || *s == '\0') {
            return std::nullopt;
        }
        return std::stod(s);
    }();
    return v;
}
}

UtilisationStats getSystemUtilisation()
{
    if (auto u = fakeUtilisation()) {
        return { *u, *u, *u };
    }
    return real_getSystemUtilisation();
}

CPUStats getCPUUtilisation()
{
    if (auto u = fakeUtilisation()) {
        // Counters that advance with the requested busy fraction
        static std::mutex mx;
        static CPUStats stats{ 0, 0 };
        std::lock_guard lock(mx);
        stats.totalCpuTime += 1000;
        stats.idleCpuTime += long((1.0 - *u) * 1000);
        return stats;
    }
    return real_getCPUUtilisation();
}

double getMemoryUtilisation()
{
    if (auto u = fakeUtilisation()) {
        return *u;
    }
    return real_getMemoryUtilisation();
}

double getLoadAverage()
{
    if (auto u = fakeUtilisation()) {
        return *u;
    }
    return real_getLoadAverage();
}
}
