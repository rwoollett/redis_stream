cmake_minimum_required(VERSION 3.12)

add_executable(ClientRedis 
        AwakenerWaitable.h
        main.cpp)

target_include_directories(ClientRedis PRIVATE
        $<BUILD_INTERFACE:${WorkQStream_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${WorkQStream_SOURCE_DIR}/clientRedis>
        )

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
target_link_libraries(ClientRedis PRIVATE Threads::Threads ${CMAKE_DL_LIBS} ${CXX_FILESYSTEM_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto WorkQConsume)

