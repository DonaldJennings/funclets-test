#pragma once
// Tracy profiler disabled for the stress harness.
#define ZoneScoped
#define ZoneScopedN(x)
#define ZoneScopedNS(x, y)
#define ZoneNamedN(a, b, c)
#define ZoneNamedNS(a, b, c, d)
#define ZoneText(a, b)
#define ZoneValue(a)
#define TracyMessageL(a)
#define FrameMark
namespace tracy {
inline void SetThreadName(const char*) {}
}
#define LockableBase(type) type
#define SharedLockableBase(type) type
#define TracyLockable(type, varname) type varname
#define TracySharedLockable(type, varname) type varname
