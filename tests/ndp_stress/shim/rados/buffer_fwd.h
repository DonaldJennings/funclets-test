#pragma once
// Minimal stand-in for Ceph's bufferlist, just enough for cephclass.cpp.
#include <cstddef>
#include <list>
#include <string>

namespace ceph {
namespace buffer {
inline namespace v15_2_0 {
class ptr
{
  public:
    explicit ptr(std::string d)
      : data(std::move(d))
    {
    }
    const char* c_str() const { return data.data(); }
    size_t length() const { return data.size(); }

  private:
    std::string data;
};

class list
{
  public:
    size_t length() const
    {
        size_t n = 0;
        for (const auto& p : bufs) {
            n += p.length();
        }
        return n;
    }
    const std::list<ptr>& buffers() const { return bufs; }
    void append(const char* d, size_t n)
    {
        bufs.emplace_back(std::string(d, n));
    }
    void clear() { bufs.clear(); }
    // Ceph's c_str() rebuilds the list into one contiguous buffer.
    const char* c_str()
    {
        if (bufs.size() > 1) {
            std::string all;
            for (const auto& p : bufs) {
                all.append(p.c_str(), p.length());
            }
            bufs.clear();
            bufs.emplace_back(std::move(all));
        }
        return bufs.empty() ? "" : bufs.front().c_str();
    }
    static list static_from_mem(char* d, size_t n)
    {
        list l;
        l.append(d, n);
        return l;
    }

  private:
    std::list<ptr> bufs;
};
}
}
using bufferlist = buffer::list;
}
