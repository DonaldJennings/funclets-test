#pragma once
// Minimal stand-in for Ceph's <rados/objclass.h>. The OSD simulator provides
// the implementations of cls_cxx_read / cls_cxx_write.
#include <cstdint>
#include <cstdio>
#include <rados/buffer_fwd.h>

typedef void* cls_handle_t;
typedef void* cls_method_handle_t;
typedef void* cls_method_context_t;
typedef int (*cls_method_cxx_call_t)(cls_method_context_t,
                                     ceph::buffer::list*,
                                     ceph::buffer::list*);

#define CLS_METHOD_RD 0x1
#define CLS_METHOD_WR 0x2

#define CLS_VER(maj, min) extern "C" int __cls_ver__##maj##_##min = 0;
#define CLS_NAME(name) extern "C" int __cls_name__##name = 0;
#define CLS_INIT(name) extern "C" void __cls_init()

extern int g_cls_log_level;
#define CLS_LOG(level, fmt, ...)                                               \
    do {                                                                       \
        if ((level) <= g_cls_log_level) {                                      \
            fprintf(stderr, "<cls> " fmt "\n", ##__VA_ARGS__);                 \
        }                                                                      \
    } while (0)

int cls_register(const char* name, cls_handle_t* handle);
int cls_register_cxx_method(cls_handle_t hclass,
                            const char* method,
                            int flags,
                            cls_method_cxx_call_t class_call,
                            cls_method_handle_t* handle);
int cls_cxx_read(cls_method_context_t hctx,
                 int ofs,
                 int len,
                 ceph::buffer::list* bl);
int cls_cxx_write(cls_method_context_t hctx,
                  int ofs,
                  int len,
                  ceph::buffer::list* bl);
