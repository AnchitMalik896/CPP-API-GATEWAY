cmake_minimum_required(VERSION 3.20)
project(ApiGateway VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT APPLE)
    message(WARNING "ApiGateway is designed for macOS (kqueue). Building on a non-Apple platform is unsupported.")
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(STATUS "Using Clang toolchain: ${CMAKE_CXX_COMPILER}")
elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    message(STATUS "Using GCC toolchain: ${CMAKE_CXX_COMPILER}")
endif()

add_library(apigateway_warnings INTERFACE)
target_compile_options(apigateway_warnings INTERFACE
    -Wall
    -Wextra
    -Werror
    -Wpedantic
    -Wshadow
    -Wconversion
    -Wsign-conversion
    -Wnon-virtual-dtor
    -Wold-style-cast
    -Wcast-align
    -Wunused
    -Woverloaded-virtual
    -Wnull-dereference
    -Wdouble-promotion
    -Wformat=2
)

add_library(apigateway_options INTERFACE)
target_compile_options(apigateway_options INTERFACE
    $<$<CONFIG:Debug>:-g -O0 -fsanitize=address,undefined>
    $<$<CONFIG:Release>:-O3>
)
target_link_options(apigateway_options INTERFACE
    $<$<CONFIG:Debug>:-fsanitize=address,undefined>
)

set(APIGATEWAY_CORE_SOURCES
    src/RateLimiter.cpp
    src/Router.cpp
    src/Socket.cpp
    src/EventLoop.cpp
    src/HttpParser.cpp
    src/HttpServer.cpp
)

add_library(apigateway_core STATIC ${APIGATEWAY_CORE_SOURCES})

target_include_directories(apigateway_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(apigateway_core
    PUBLIC
        apigateway_warnings
        apigateway_options
)

find_package(Threads REQUIRED)
target_link_libraries(apigateway_core PUBLIC Threads::Threads)

add_executable(apigateway src/main.cpp)
target_link_libraries(apigateway PRIVATE apigateway_core)
