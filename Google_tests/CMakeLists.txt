cmake_minimum_required(VERSION 3.26)
project(Google_tests)

add_executable(Google_Tests_run ExternalRunTest.cpp SortTest.cpp DistinctTest.cpp FilterTest.cpp SortDistinctTest.cpp GroupByTest.cpp HashDistinctTest.cpp HashGroupByTest.cpp)
target_link_libraries(Google_Tests_run gtest gtest_main libovc)