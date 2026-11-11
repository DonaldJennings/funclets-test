# Stand-alone stress harness for the NDP socket protocol between the Ceph
# object class (src/cephclass) and the Faasm storage worker
# (src/wasm/ndp_endpoint.cpp). It compiles the real sources from FAASM_ROOT
# with small stand-ins for librados/objclass and the faabric scheduler, so it
# builds without Ceph, WAVM or the rest of the Faasm toolchain.
#
#   cmake -S tests/ndp_stress -B build-ndp -DFAASM_ROOT=$PWD
#   cmake --build build-ndp
#
# Ubuntu packages: libboost-dev libflatbuffers-dev flatbuffers-compiler
#                  libspdlog-dev libfmt-dev libabsl-dev
cmake_minimum_required(VERSION 3.20)
project(ndp_stress CXX)
# Build with clang like the rest of Faasm: GCC 13 rejects the swap() in
# faabric's concurrent_map.h.

set(FAASM_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." CACHE PATH "Faasm source tree to test")
set(FAABRIC_ROOT "${FAASM_ROOT}/faabric" CACHE PATH "faabric source tree")
option(NDP_LEGACY_API "Source tree uses the pre-fix ndp.h API" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

find_package(Threads REQUIRED)
find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)
find_package(absl REQUIRED)
find_package(Boost REQUIRED)
find_program(FLATC flatc REQUIRED)

set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/gen)
add_custom_command(
    OUTPUT ${GEN_DIR}/cephcomm_generated.h
    COMMAND ${CMAKE_COMMAND} -E make_directory ${GEN_DIR}
    COMMAND ${FLATC} --cpp -o ${GEN_DIR} ${FAASM_ROOT}/src/cephclass/cephcomm.fbs
    DEPENDS ${FAASM_ROOT}/src/cephclass/cephcomm.fbs)
add_custom_target(ndp_flatbuf DEPENDS ${GEN_DIR}/cephcomm_generated.h)

set(SHIM_DIR ${CMAKE_CURRENT_SOURCE_DIR}/shim)

# --- Simulated OSD with the real object class ---
add_executable(osd_sim
    osd_sim.cpp
    ${FAASM_ROOT}/src/cephclass/cephclass.cpp)
add_dependencies(osd_sim ndp_flatbuf)
# Ceph's objclass.h needs C++17, and so does cephclass.cpp in the real build
set_source_files_properties(${FAASM_ROOT}/src/cephclass/cephclass.cpp
    PROPERTIES COMPILE_OPTIONS "-std=c++17")
target_include_directories(osd_sim PRIVATE
    ${SHIM_DIR} ${GEN_DIR} ${FAASM_ROOT}/include)
target_link_libraries(osd_sim PRIVATE spdlog::spdlog fmt::fmt Threads::Threads)

# --- Simulated storage worker with the real NDP endpoint ---
add_executable(storage_sim
    storage_sim.cpp
    ${FAASM_ROOT}/src/wasm/ndp_endpoint.cpp
    sim_system_metrics.cpp
    ${FAABRIC_ROOT}/src/util/system_metrics.cpp
    ${FAABRIC_ROOT}/src/util/environment.cpp)
set_source_files_properties(${FAABRIC_ROOT}/src/util/system_metrics.cpp
    PROPERTIES COMPILE_DEFINITIONS
    "getSystemUtilisation=real_getSystemUtilisation;getCPUUtilisation=real_getCPUUtilisation;getMemoryUtilisation=real_getMemoryUtilisation;getLoadAverage=real_getLoadAverage")
add_dependencies(storage_sim ndp_flatbuf)
# The shim directory comes first so it shadows the heavy faabric headers
target_include_directories(storage_sim BEFORE PRIVATE
    ${SHIM_DIR} ${GEN_DIR} ${FAASM_ROOT}/include ${FAABRIC_ROOT}/include)
target_link_libraries(storage_sim PRIVATE
    spdlog::spdlog fmt::fmt Threads::Threads Boost::headers
    absl::flat_hash_map absl::hash)
if(NDP_LEGACY_API)
    # The old ndp.h uses tl::expected (from conan in the real build)
    set(TL_DIR ${CMAKE_CURRENT_BINARY_DIR}/tl-expected)
    file(DOWNLOAD
        https://raw.githubusercontent.com/TartanLlama/expected/v1.1.0/include/tl/expected.hpp
        ${TL_DIR}/tl/expected.hpp
        EXPECTED_HASH SHA256=5a6a46da2e46ee3360bd8fbd759affefc46b8bfd6295043262ec867acac1fa14)
    target_include_directories(storage_sim PRIVATE ${TL_DIR})
    target_compile_definitions(storage_sim PRIVATE NDP_LEGACY_API)
endif()
