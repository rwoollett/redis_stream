cmake_minimum_required(VERSION 3.13)
project(NetworkRedis VERSION 1.0
        DESCRIPTION "Redis on network"
        LANGUAGES C CXX)

# Set module path for Tester Catch2 cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")

# set everything up for c++ 20 features - require coroutines and BOOST_ASIO_HAS_CO_AWAIT
set(CMAKE_CXX_STANDARD 20) 
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# test that filesystem header actually is there and works
try_compile(HAS_FS "${CMAKE_BINARY_DIR}/temp"
        "${CMAKE_SOURCE_DIR}/cmake/has_stdfs.cc"
        CMAKE_FLAGS -DCMAKE_CXX_STANDARD=17 -DCMAKE_CXX_STANDARD_REQUIRED=ON )

#message("Supported features = ${CMAKE_CXX_COMPILE_FEATURES}")
if (HAS_FS)
    message(STATUS "std::filesystem supported")
else()
    message(STATUS "Using std::experimental::filesystem instead of std::filesystem")
    #set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -lstdc++fs")
    add_compile_definitions(USE_EXP_FILESYSTEM)
endif()

# FetchContent added in CMake 3.11, downloads during the configure step
include(FetchContent)
# FetchContent_MakeAvailable was not added until CMake 3.14; use our shim
if(${CMAKE_VERSION} VERSION_LESS 3.14)
    include(cmake/add_FetchContent_MakeAvailable.cmake)
endif()

#add_compile_definitions(LOKI_FUNCTOR_IS_NOT_A_SMALLOBJECT HAVE_SQLITE)
#add_compile_definitions(LOKI_CLASS_LEVEL_THREADING LOKI_FUNCTOR_IS_NOT_A_SMALLOBJECT HAVE_SQLITE)
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_definitions(HAVE_ASIO)
endif()

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_DEBUG_LIBS OFF)
set(Boost_USE_RELEASE_LIBS ON) 
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME ON) 
set(Boost_NO_BOOST_CMAKE ON)
set(BOOST_ROOT /usr/local)
find_package(Boost 1.86 REQUIRED)

message(STATUS "Boost include directory:  ${Boost_INCLUDE_DIR}")

add_subdirectory(clientRedis)

add_subdirectory(clientPublisher)

add_subdirectory(redisSubscribe)

add_subdirectory(redisPublish)

install(TARGETS ClientRedis EXPORT NetworkRedisTargets RUNTIME DESTINATION bin)
install(EXPORT NetworkRedisTargets
        FILE NetworkRedisTargets.cmake
        DESTINATION bin/cmake/NetworkRedis)



