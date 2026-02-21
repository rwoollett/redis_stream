cmake_minimum_required(VERSION 3.13)
project(WorkQStream VERSION 1.0
        DESCRIPTION "Redis stream for work queue produce and consume with ack message"
        LANGUAGES C CXX)

# set everything up for c++ 20 features - require coroutines and BOOST_ASIO_HAS_CO_AWAIT
set(CMAKE_CXX_STANDARD 20) 
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include(FetchContent)
add_compile_definitions(HAVE_ASIO)

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_DEBUG_LIBS OFF)
set(Boost_USE_RELEASE_LIBS ON) 
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME ON) 
set(Boost_NO_BOOST_CMAKE ON)
set(BOOST_ROOT /usr/local)
find_package(Boost 1.86 REQUIRED)

option(BUILD_WORKQ_TESTS "Build tests and include clientProducer and clientRedis" OFF)

add_subdirectory(workqstream/common)
add_subdirectory(workqstream/consume)
add_subdirectory(workqstream/produce)

if (BUILD_WORKQ_TESTS)
  include(FetchContent)
  FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
  )
  FetchContent_MakeAvailable(nlohmann_json)

  target_link_libraries(WorkQProduce PUBLIC
    nlohmann_json::nlohmann_json 
  )
  target_link_libraries(WorkQConsume PUBLIC
    nlohmann_json::nlohmann_json 
  )

  add_subdirectory(clientRedis)
  add_subdirectory(clientProducer)
endif()



