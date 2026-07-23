cmake_minimum_required(VERSION 3.20)
project(apigateway VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MSVC)
    add_compile_options(/W4 /WX)
else()
    add_compile_options(-Wall -Wextra -Werror -pedantic)
endif()

find_package(Threads REQUIRED)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

set(APIGATEWAY_CORE_SOURCES
    src/Socket.cpp
    src/RateLimiter.cpp
    src/Router.cpp
    src/ThreadPool.cpp
    src/ApiGateway.cpp
    src/EventLoop.cpp
    src/HttpParser.cpp
    src/HttpServer.cpp
)

add_library(apigateway_core STATIC ${APIGATEWAY_CORE_SOURCES})
target_link_libraries(apigateway_core PUBLIC Threads::Threads)

add_executable(apigateway src/main.cpp)
target_link_libraries(apigateway PRIVATE apigateway_core)