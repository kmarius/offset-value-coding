cmake_minimum_required(VERSION 3.26)
project(Google_tests)

add_executable(Google_Tests_run ExternalRunTest.cpp SortTest.cpp InStreamDistinctTest.cpp FilterTest.cpp SortDistinctTest.cpp InStreamGroupByTest.cpp HashDistinctTest.cpp HashGroupByTest.cpp InSortGroupByTest.cpp)
target_link_libraries(Google_Tests_run gtest gtest_main libovc)