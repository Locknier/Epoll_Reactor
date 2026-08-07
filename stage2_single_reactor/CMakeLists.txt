cmake_minimum_required(VERSION 3.10)
project(stage2_single_reactor C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g")

add_executable(stage2_single_reactor
    main.c
    reactor.c
)
