cmake_minimum_required(VERSION 3.10)
project(stage4_main_sub_reactor C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g -pthread")

add_executable(stage4_main_sub_reactor
    main.c
    main_reactor.c
    sub_reactor.c
)

target_link_libraries(stage4_main_sub_reactor pthread)
