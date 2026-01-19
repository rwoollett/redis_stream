cmake_minimum_required(VERSION 3.12)
# Target NetProcessor
add_executable(ClientRedis 
        io_utility/io_utility.h
        io_utility/io_utility.cpp
        AwakenerWaitable.h
        main.cpp)

message(STATUS "Boost include directory:  ${CMAKE_CXX_COMPILE_FEATURES}")
#target_compile_features(ClientRedis PUBLIC cxx_std_20) use c++20

target_include_directories(ClientRedis PRIVATE
        $<BUILD_INTERFACE:${NetworkRedis_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${NetworkRedis_SOURCE_DIR}/ClientRedis>
        )

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # using Clang
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  # using GCC
  message(STATUS "Console has FS: ${HAS_FS}")
  set(CXX_FILESYSTEM_LIBRARIES "stdc++fs")
  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
  find_package(OpenSSL REQUIRED)
  target_link_libraries(ClientRedis PRIVATE Threads::Threads ${CMAKE_DL_LIBS} ${CXX_FILESYSTEM_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto RedisSubscribe)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
    # using Intel C++
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    # using Visual Studio C++
    target_link_libraries(ClientRedis NetworkServer)
endif()

