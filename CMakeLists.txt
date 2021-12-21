cmake_minimum_required(VERSION 3.17)
project(task_executor)

set(CMAKE_C_STANDARD 99)
add_executable(task_executor_c main.c)


set(CMAKE_CXX_STANDARD 17)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer")
add_executable(task_executor_cpp main.cpp)
