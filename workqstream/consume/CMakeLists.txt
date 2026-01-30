
add_library(WorkQConsume
    Consumer.cpp
    Consumer.h
)

add_library(WorkQStream::WorkQConsume ALIAS WorkQConsume)

target_compile_features(WorkQConsume PUBLIC cxx_std_20)

target_include_directories(WorkQConsume
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../..      # <workqstream/...>
    PRIVATE
        $<BUILD_INTERFACE:${Boost_INCLUDE_DIR}>
)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(WorkQConsume PRIVATE Threads::Threads ${CMAKE_DL_LIBS})


