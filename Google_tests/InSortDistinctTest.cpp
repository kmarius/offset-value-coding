#include "lib/Row.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/IncreasingRangeGenerator.h"
#include "lib/iterators/InSortDistinct.h"
#include "lib/iterators/VectorScan.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class SortDistinctTest : public ::testing::Test {
protected:

    const size_t QUEUE_SIZE = QUEUE_CAPACITY;
    const size_t INITIAL_RUNS = (1 << RUN_IDX_BITS) - 3;
    const size_t SEED = 1337;

    void SetUp() override {
        log_set_quiet(true);
        log_set_level(LOG_ERROR);
    }

    void TearDown() override {

    }

    void testInSortDistinct(size_t num_rows) const {
        auto plan = new AssertSortedUnique(new InSortDistinct(new IncreasingRangeGenerator(num_rows, 100, SEED, true)));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        delete plan;
    }

    void testInSortDistinctNoOvc(size_t num_rows) const {
        auto plan = new AssertSortedUnique(new InSortDistinctNoOvc(new IncreasingRangeGenerator(num_rows, 100, SEED, true)));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        delete plan;
    }
};

TEST_F(SortDistinctTest, EmptyTest) {
    auto *plan = new AssertSortedUnique(new InSortDistinct(new IncreasingRangeGenerator(0, 100, SEED)));
    plan->run();
    ASSERT_TRUE(plan->isSortedAndUnique());
    ASSERT_EQ(plan->count(), 0);
    delete plan;
}

TEST_F(SortDistinctTest, DetectUnsorted) {
    auto *plan = new AssertSortedUnique(new VectorScan(
            {
                    {0, 0, {1, 0, 0, 0}},
                    {0, 0, {0, 0, 0, 0}}
            }
    ));
    plan->run();
    ASSERT_FALSE(plan->isSortedAndUnique());
    delete plan;
}

TEST_F(SortDistinctTest, DetectDuplicate) {
    auto *plan = new AssertSortedUnique(new VectorScan(
            {
                    {801, 0, {1, 0, 0, 0}},
                    {0,   0, {1, 0, 0, 0}}
            }
    ));
    plan->run();
    ASSERT_FALSE(plan->isSortedAndUnique());
    delete plan;
}

TEST_F(SortDistinctTest, SortEmpty) {
    testInSortDistinct(0);
}

TEST_F(SortDistinctTest, SortOne) {
    testInSortDistinct(1);
}

TEST_F(SortDistinctTest, SortTwo) {
    testInSortDistinct(2);
}

TEST_F(SortDistinctTest, SortTiny) {
    testInSortDistinct(5);
}

TEST_F(SortDistinctTest, SortSmall) {
    testInSortDistinct(QUEUE_SIZE);
}

TEST_F(SortDistinctTest, SortSmall2) {
    testInSortDistinct(QUEUE_SIZE * 3 / 2);
}

TEST_F(SortDistinctTest, SortSmallish) {
    testInSortDistinct(QUEUE_SIZE * 3);
}

TEST_F(SortDistinctTest, SortMedium) {
    testInSortDistinct(INITIAL_RUNS * QUEUE_SIZE);
}

TEST_F(SortDistinctTest, SortMediumButSmaller) {
    testInSortDistinct(INITIAL_RUNS * QUEUE_SIZE - QUEUE_SIZE / 2);
}

TEST_F(SortDistinctTest, SortMediumButABitLarger) {
    testInSortDistinct(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(SortDistinctTest, SortMediumButABitLarger2) {
    testInSortDistinct(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE * 3 / 2);
}

TEST_F(SortDistinctTest, SortLarge) {
    testInSortDistinct(INITIAL_RUNS * QUEUE_SIZE * 8);
}

TEST_F(SortDistinctTest, SortEmptyNoOvc) {
    testInSortDistinctNoOvc(0);
}

TEST_F(SortDistinctTest, SortOneNoOvc) {
    testInSortDistinctNoOvc(1);
}

TEST_F(SortDistinctTest, SortTwoNoOvc) {
    testInSortDistinctNoOvc(2);
}

TEST_F(SortDistinctTest, SortTinyNoOvc) {
    testInSortDistinctNoOvc(5);
}

TEST_F(SortDistinctTest, SortSmallNoOvc) {
    testInSortDistinctNoOvc(QUEUE_SIZE);
}

TEST_F(SortDistinctTest, SortSmallNoOvc2) {
    testInSortDistinctNoOvc(QUEUE_SIZE * 3 / 2);
}

TEST_F(SortDistinctTest, SortSmallishNoOvc) {
    testInSortDistinctNoOvc(QUEUE_SIZE * 3);
}

TEST_F(SortDistinctTest, SortMediumNoOvc) {
    testInSortDistinctNoOvc(INITIAL_RUNS * QUEUE_SIZE);
}

//TEST_F(SortDistinctTest, SortMediumButSmallerNoOvc) {
//    testInSortDistinctNoOvc(INITIAL_RUNS * QUEUE_SIZE - QUEUE_SIZE / 2);
//}
//
//TEST_F(SortDistinctTest, SortMediumButABitLargerNoOvc) {
//    testInSortDistinctNoOvc(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE / 2);
//}

TEST_F(SortDistinctTest, SortMediumButABitLargerNoOvc2) {
    testInSortDistinctNoOvc(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE * 3 / 2);
}

//TEST_F(SortDistinctTest, SortLargeNoOvc) {
//    testInSortDistinctNoOvc(INITIAL_RUNS * QUEUE_SIZE * 8);
//}