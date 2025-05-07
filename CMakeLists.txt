message("hello world")

cmake_minimum_required(VERSION 3.10)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(KV
    VERSION 2025.01
    DESCRIPTION "KV Store"
    LANGUAGES C CXX
)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to `Debug` as none was specified.")
    set(CMAKE_BUILD_TYPE "Debug")
endif()

message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

# Compiler flags
set(CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS_DEBUG} -Werror -Wall -Wextra -pedantic -stdlib=libc++ -O0 -ggdb -fno-exceptions  -Wno-zero-length-array -Wno-reinterpret-base-class")

# set(CMAKE_CXX_FLAGS_DEBUG
#     "${CMAKE_CXX_FLAGS_DEBUG} -Wno-unused -Wno-unused-parameter")

# Sanitizer flags
set(CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined -g -fno-omit-frame-pointer -fno-sanitize=alignment"
)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_EXE_LINKER_FLAGS: ${CMAKE_EXE_LINKER_FLAGS}")
message(STATUS "CMAKE_SHARED_LINKER_FLAGS: ${CMAKE_SHARED_LINKER_FLAGS}")

# Output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# Gather source files
file(GLOB_RECURSE SOURCE_FILES
    "${PROJECT_SOURCE_DIR}/src/*.cc"
)

message(STATUS "Compiling the following source files:")
foreach(FILE ${SOURCE_FILES})
    message(STATUS "${FILE}")
endforeach()

# add_library(kv ${SOURCE_FILES})

# Fetch fmt from GitHub
include(FetchContent)
FetchContent_Declare(
  fmt
  GIT_REPOSITORY "https://github.com/fmtlib/fmt.git"
  GIT_TAG "10.2.1"
)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest fmt)

# Link fmt to target
# target_link_libraries(kv PRIVATE fmt::fmt)

# Include directory
# target_include_directories(kv PRIVATE "${PROJECT_SOURCE_DIR}/kv")

add_subdirectory(test)
