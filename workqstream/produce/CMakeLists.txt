add_library(WorkQProduce
    Producer.cpp
    Producer.h
)

add_library(WorkQStream::WorkQProduce ALIAS WorkQProduce)

target_compile_features(WorkQProduce PUBLIC cxx_std_20)

target_include_directories(WorkQProduce
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../..      # <workqstream/...>
    PRIVATE
        $<BUILD_INTERFACE:${Boost_INCLUDE_DIR}>
)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(WorkQConsume PRIVATE Threads::Threads ${CMAKE_DL_LIBS})


