cmake_minimum_required(VERSION 3.12)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")

  add_library(RedisPublish
    Publish.cpp
    Publish.h
  )

  target_include_directories(RedisPublish PRIVATE
    $<BUILD_INTERFACE:${Boost_INCLUDE_DIR}>
  )

  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
  target_link_libraries(RedisPublish PRIVATE Threads::Threads ${CMAKE_DL_LIBS})

endif()

