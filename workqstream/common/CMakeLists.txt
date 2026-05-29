file(GLOB WORKQ_COMMON_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.h"
)

add_library(WorkQCommon STATIC
    ${WORKQ_COMMON_SOURCES}
)

add_library(WorkQStream::WorkQCommon ALIAS WorkQCommon)

target_include_directories(WorkQCommon
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include/redis_stream/common>
)

target_link_libraries(WorkQCommon
    PUBLIC
        MTLog::MTLog
        nlohmann_json::nlohmann_json
)

install(TARGETS WorkQCommon
    EXPORT WorkQStreamTargets
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
    DESTINATION include/redis_stream/common
    FILES_MATCHING PATTERN "*.h"
)
