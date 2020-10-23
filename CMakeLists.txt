cmake_minimum_required(VERSION 3.17)
project(Concurrent)

set(CMAKE_CXX_STANDARD 17)

add_library(Concurrent SHARED threadpool.cpp threadpool.h)