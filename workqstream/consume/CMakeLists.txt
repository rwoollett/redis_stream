cmake_minimum_required(VERSION 3.12)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")

  add_library(WorkQConsume
    Consumer.cpp
    Consumer.h
  )

  target_include_directories(WorkQConsume PRIVATE
    $<BUILD_INTERFACE:${Boost_INCLUDE_DIR}>
  )

  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
  target_link_libraries(WorkQConsume PRIVATE Threads::Threads ${CMAKE_DL_LIBS})

endif()

