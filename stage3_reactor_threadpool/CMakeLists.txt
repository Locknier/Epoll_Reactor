cmake_minimum_required(VERSION 3.10)
project(stage3_reactor_threadpool C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g -pthread")

add_executable(stage3_reactor_threadpool
    main.c
    reactor.c
    threadpool.c
)

target_link_libraries(stage3_reactor_threadpool pthread)
