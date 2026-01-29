cmake_minimum_required(VERSION 3.12)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")

  add_library(WorkQProduce
    Producer.cpp
    Producer.h
  )

  target_include_directories(WorkQProduce PRIVATE
    $<BUILD_INTERFACE:${Boost_INCLUDE_DIR}>
  )

  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
  target_link_libraries(WorkQProduce PRIVATE Threads::Threads ${CMAKE_DL_LIBS})

endif()

