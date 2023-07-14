#include "lib/Row.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/Generator.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/Distinct.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class DistinctTest : public ::testing::Test {
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

    void testDedup(size_t num_rows) {
        auto *plan = new AssertSortedUnique(
                new Distinct(
                        new Sort(
                                new Generator(num_rows, 100, SEED, true))));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        delete plan;
    }
};

TEST_F(DistinctTest, EmptyTest) {
    auto *plan = new AssertSortedUnique(new Generator(0, 100, SEED));
    plan->run();
    ASSERT_TRUE(plan->isSortedAndUnique());
    ASSERT_EQ(plan->count(), 0);
    delete plan;
}

TEST_F(DistinctTest, DetectUnsorted) {
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

TEST_F(DistinctTest, DetectDupe) {
    auto *plan = new AssertSortedUnique(new VectorScan(
            {
                    {0, 0, {0, 0, 0, 0}},
                    {0, 0, {0, 0, 0, 1}},
                    {0, 0, {0, 0, 0, 1}},
                    {0, 0, {0, 0, 0, 2}}
            }
    ));
    plan->run();
    ASSERT_FALSE(plan->isSortedAndUnique());
    delete plan;
}

TEST_F(DistinctTest, DedupEmpty) {
    testDedup(0);
}

TEST_F(DistinctTest, DedupOne) {
    testDedup(1);
}

TEST_F(DistinctTest, DedupTwo) {
    testDedup(2);
}

TEST_F(DistinctTest, DedupTiny) {
    testDedup(5);
}

TEST_F(DistinctTest, DedupSmall) {
    testDedup(QUEUE_SIZE);
}

TEST_F(DistinctTest, DedupSmall2) {
    testDedup(QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(DistinctTest, DedupSmallish) {
    testDedup(QUEUE_SIZE * 3);
}

TEST_F(DistinctTest, DedupMedium) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS);
}

TEST_F(DistinctTest, DedupMediumButSmaller) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS - QUEUE_SIZE / 2);
}

TEST_F(DistinctTest, DedupMediumButABitLarger) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE / 2);
}

TEST_F(DistinctTest, DedupMediumButABitLarger2) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE * 3 / 2);
}

TEST_F(DistinctTest, DedupLarge) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS * 8);
}