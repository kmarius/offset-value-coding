cmake_minimum_required(VERSION 3.26)
project(Google_tests)

add_executable(Google_Tests_run ExternalRunTest.cpp SortTest.cpp DedupTest.cpp FilterTest.cpp)
target_link_libraries(Google_Tests_run gtest gtest_main libovc)