
file(GLOB WORKQ_RECOVERY_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/*.h"
)

add_library(WorkQRecovery STATIC
    ${WORKQ_RECOVERY_SOURCES}
)

add_library(WorkQStream::WorkQRecovery ALIAS WorkQRecovery)

target_compile_features(WorkQRecovery PUBLIC cxx_std_20)

target_include_directories(WorkQRecovery
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../..>
        $<INSTALL_INTERFACE:include/workqstream/recovery>
    PRIVATE
        $<BUILD_INTERFACE:${Boost_INCLUDE_DIR}>
)

target_link_libraries(WorkQRecovery
    PUBLIC
        MTLog::MTLog
        nlohmann_json::nlohmann_json
    PRIVATE
        WorkQStream::WorkQCommon
)

if (WIN32)
    find_package(OpenSSL REQUIRED)
    target_link_libraries(WorkQRecovery
        PRIVATE
            OpenSSL::SSL
            OpenSSL::Crypto
            ws2_32
    )
endif()

install(TARGETS WorkQRecovery
    EXPORT WorkQStreamTargets
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/
    DESTINATION include/workqstream/recovery
    FILES_MATCHING PATTERN "*.h"
)
