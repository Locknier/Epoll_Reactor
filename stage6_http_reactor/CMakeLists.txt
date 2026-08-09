cmake_minimum_required(VERSION 3.10)
project(stage6_http_reactor C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g -pthread")

add_executable(stage6_http_reactor
    main.c
    main_reactor.c
    sub_reactor.c
    threadpool.c
    http_parser.c
)

target_link_libraries(stage6_http_reactor pthread)
