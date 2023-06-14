cmake_minimum_required(VERSION 3.26)
project(ovc)

set(CMAKE_CXX_STANDARD 14)

# The following are needed for profiling
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")

# Enable ASAN:
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")

set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -flto -g -DNDEBUG")

include_directories(src)

add_subdirectory(Google_tests)

add_library(libovc
        STATIC
        src/lib/defs.h
        src/lib/io/Buffer.h
        src/lib/io/BufferManager.cpp
        src/lib/io/BufferManager.h
        src/lib/io/ExternalRunR.cpp
        src/lib/io/ExternalRunR.h
        src/lib/io/ExternalRunRS.cpp
        src/lib/io/ExternalRunRS.h
        src/lib/io/ExternalRunW.cpp
        src/lib/io/ExternalRunW.h
        src/lib/io/ExternalRunWS.cpp
        src/lib/io/ExternalRunWS.h
        src/lib/iterators/AssertSorted.cpp
        src/lib/iterators/AssertSorted.h
        src/lib/iterators/Filter.cpp
        src/lib/iterators/Filter.h
        src/lib/iterators/Generator.cpp
        src/lib/iterators/Generator.h
        src/lib/iterators/Iterator.cpp
        src/lib/iterators/Iterator.h
        src/lib/iterators/Scan.cpp
        src/lib/iterators/Scan.h
        src/lib/iterators/Sort.cpp
        src/lib/iterators/Sort.h
        src/lib/log.cpp
        src/lib/log.h
        src/lib/PriorityQueue.cpp
        src/lib/PriorityQueue.h
        src/lib/Row.cpp
        src/lib/Row.h
        src/lib/Run.cpp
        src/lib/Run.h
        src/lib/utils.h
        src/lib/iterators/VectorScan.cpp
        src/lib/iterators/VectorScan.h src/lib/Schema.cpp src/lib/Schema.h src/lib/iterators/Dedup.cpp src/lib/iterators/Dedup.h src/lib/iterators/AssertSortedUnique.h src/lib/iterators/AssertSortedUnique.cpp src/lib/iterators/AssertEqual.cpp src/lib/iterators/AssertEqual.h src/lib/iterators/HashDedup.cpp src/lib/HashSet.cpp src/lib/HashSet.h)
target_link_libraries(libovc uring)

add_executable(ovc src/ovc/main.cpp)
target_link_libraries(ovc libovc)

add_executable(rcat src/rcat/main.cpp)
target_link_libraries(rcat libovc)

add_executable(rcheck src/rcheck/main.cpp)
target_link_libraries(rcheck libovc)

add_executable(rcount src/rcount/main.cpp)
target_link_libraries(rcount libovc)

add_executable(rgen src/rgen/main.cpp)
target_link_libraries(rgen libovc)

add_custom_target(BuildAll)
add_dependencies(BuildAll
        libovc
        ovc
        rcat
        rcheck
        rcount
        rgen
        )
