cmake_minimum_required(VERSION 3.12)

add_executable(ClientProducer  main.cpp)

target_include_directories(ClientProducer PRIVATE
        $<BUILD_INTERFACE:${WorkQStream_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${WorkQStream_SOURCE_DIR}/clientProducer>
        )

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
target_link_libraries(ClientProducer PRIVATE Threads::Threads OpenSSL::SSL OpenSSL::Crypto WorkQProduce)

