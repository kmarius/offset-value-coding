#include "lib/Row.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/Generator.h"
#include "lib/iterators/Sort.h"
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

    void testSorted(size_t num_rows) {
        auto plan = new AssertSortedUnique(new SortDistinct(new Generator(num_rows, 100, SEED, true)));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        delete plan;
    }
};

TEST_F(SortDistinctTest, EmptyTest) {
    auto *plan = new AssertSortedUnique(new SortDistinct(new Generator(0, 100, SEED)));
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
                    {0, 0, {1, 0, 0, 0}}
            }
    ));
    plan->run();
    ASSERT_FALSE(plan->isSortedAndUnique());
    delete plan;
}

TEST_F(SortDistinctTest, SortEmpty) {
    testSorted(0);
}

TEST_F(SortDistinctTest, SortOne) {
    testSorted(1);
}

TEST_F(SortDistinctTest, SortTwo) {
    testSorted(2);
}

TEST_F(SortDistinctTest, SortTiny) {
    testSorted(5);
}

TEST_F(SortDistinctTest, SortSmall) {
    testSorted(QUEUE_SIZE);
}

TEST_F(SortDistinctTest, SortSmall2) {
    testSorted(QUEUE_SIZE * 3 / 2);
}

TEST_F(SortDistinctTest, SortSmallish) {
    testSorted(QUEUE_SIZE * 3);
}

TEST_F(SortDistinctTest, SortMedium) {
    testSorted(INITIAL_RUNS * QUEUE_SIZE);
}

TEST_F(SortDistinctTest, SortMediumButSmaller) {
    testSorted(INITIAL_RUNS * QUEUE_SIZE - QUEUE_SIZE / 2);
}

TEST_F(SortDistinctTest, SortMediumButABitLarger) {
    testSorted(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(SortDistinctTest, SortMediumButABitLarger2) {
    testSorted(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE * 3 / 2);
}

TEST_F(SortDistinctTest, SortLarge) {
    testSorted(INITIAL_RUNS * QUEUE_SIZE * 8);
}