file(GLOB WORKQ_PRODUCE_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.h"
)

add_library(WorkQProduce STATIC
    ${WORKQ_PRODUCE_SOURCES}
)

add_library(WorkQStream::WorkQProduce ALIAS WorkQProduce)

target_compile_features(WorkQProduce PUBLIC cxx_std_20)

target_include_directories(WorkQProduce
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../..>
        $<INSTALL_INTERFACE:include/redis_stream>
    PRIVATE
        $<BUILD_INTERFACE:${Boost_INCLUDE_DIR}>
)

target_link_libraries(WorkQProduce
    PUBLIC
        MTLog::MTLog
        nlohmann_json::nlohmann_json
    PRIVATE
        WorkQStream::WorkQCommon
)

install(TARGETS WorkQProduce
    EXPORT WorkQStreamTargets
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
    DESTINATION include/redis_stream/produce
    FILES_MATCHING PATTERN "*.h"
)
