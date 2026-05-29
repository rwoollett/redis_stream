
file(GLOB WORKQ_CONSUME_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.h"
)

add_library(WorkQConsume STATIC
    ${WORKQ_CONSUME_SOURCES}
)

add_library(WorkQStream::WorkQConsume ALIAS WorkQConsume)

target_compile_features(WorkQConsume PUBLIC cxx_std_20)

target_include_directories(WorkQConsume
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../..>
        $<INSTALL_INTERFACE:include/redis_stream>
    PRIVATE
        $<BUILD_INTERFACE:${Boost_INCLUDE_DIR}>
)

target_link_libraries(WorkQConsume
    PUBLIC
        MTLog::MTLog
        nlohmann_json::nlohmann_json
    PRIVATE
        WorkQStream::WorkQCommon
)

install(TARGETS WorkQConsume
    EXPORT WorkQStreamTargets
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
    DESTINATION include/redis_stream/consume
    FILES_MATCHING PATTERN "*.h"
)
