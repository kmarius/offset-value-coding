cmake_minimum_required(VERSION 3.26)
project(ovc)

set(CMAKE_CXX_STANDARD 17)

# The following are needed for profiling
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")

# Enable ASAN:
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")

set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_PROFILE "-O0 -g -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -flto -g -DNDEBUG")

include_directories(src)

add_subdirectory(Google_tests)

if (NOT COLLECT_STATS)
        set(COLLECT_STATS 0)
endif()
if(COLLECT_STATS)
else()
        add_definitions(-DNO_COLLECT_STATS)
endif()

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
        src/lib/iterators/AssertSorted.h
        src/lib/iterators/Filter.h
        src/lib/iterators/IncreasingRangeGenerator.cpp
        src/lib/iterators/IncreasingRangeGenerator.h
        src/lib/iterators/Iterator.cpp
        src/lib/iterators/Iterator.h
        src/lib/iterators/Scan.h
        src/lib/iterators/Sort.h
        src/lib/log.cpp
        src/lib/log.h
        src/lib/PriorityQueue.h
        src/lib/Row.cpp
        src/lib/Row.h
        src/lib/Run.h
        src/lib/utils.h
        src/lib/iterators/VectorScan.h src/lib/iterators/InStreamDistinct.h src/lib/iterators/AssertSortedUnique.h src/lib/iterators/AssertEqual.h src/lib/iterators/HashDistinct.cpp src/lib/Partitioner.cpp src/lib/Partitioner.h src/lib/iterators/PrefixTruncationCounter.cpp src/lib/iterators/PrefixTruncationCounter.h src/lib/iterators/OVCApplier.h src/lib/iterators/RowGenerator.h src/lib/iterators/RowGenerator.cpp src/lib/iterators/Sort.ipp src/lib/iterators/InStreamGroupBy.h src/lib/iterators/HashGroupBy.ipp src/lib/iterators/HashGroupBy.h src/lib/iterators/Shuffle.cpp src/lib/iterators/Shuffle.h src/lib/PriorityQueue.ipp src/lib/iterators/LeftSemiJoin.h src/lib/iterators/LeftSemiHashJoin.cpp src/lib/iterators/LeftSemiHashJoin.h src/lib/iterators/Multiplier.h src/lib/iterators/DuplicateGenerator.h src/lib/iterators/ApproximateDuplicateGenerator.h src/lib/iterators/InSortGroupBy.h src/lib/aggregates.h
        src/lib/utils.cpp
        src/lib/iterators/AssertCorrectOVC.h
        src/lib/iterators/Transposer.h
        src/lib/iterators/GeneratorWithDomains.cpp
        src/lib/iterators/GeneratorWithDomains.h
        src/lib/iterators/UniqueRowGenerator.h
        src/lib/iterators/SegmentedSort.h
        src/lib/iterators/AssertCount.h
        src/lib/iterators/VectorGen.h
        src/paper1/main.cpp
        src/lib/iterators/SegmentedGen.h
        src/lib/iterators/RandomGenerator.h
        src/lib/iterators/RowBuffer.h
        src/lib/iterators/SegmentedSortNoRuns.h)

target_link_libraries(libovc uring)

add_executable(paper1 src/paper1/main.cpp)
target_link_libraries(paper1 libovc)

add_executable(paper2 src/paper2/main.cpp)
target_link_libraries(paper2 libovc)

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
        paper1
        paper2
        rcat
        rcheck
        rcount
        rgen
        )
