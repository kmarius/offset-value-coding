project(Google_tests)

add_executable(Google_Tests_run ExternalRunTest.cpp IteratorTest.cpp)
target_link_libraries(Google_Tests_run gtest gtest_main libovc)