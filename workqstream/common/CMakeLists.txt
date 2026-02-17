
add_library(WorkQCommon
    json.h
    Common.h
    Groups.cpp
    Groups.h
)

add_library(WorkQStream::WorkQCommon ALIAS WorkQCommon)

target_compile_features(WorkQCommon PUBLIC cxx_std_20)

target_include_directories(WorkQCommon
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../..      # <workqstream/...>
    PRIVATE
        $<BUILD_INTERFACE:${Boost_INCLUDE_DIR}>
)


