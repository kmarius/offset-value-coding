#include "lib/Row.h"
#include "lib/iterators/HashDistinct.h"
#include "lib/log.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/Generator.h"
#include "lib/iterators/VectorScan.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class HashDistinctTest : public ::testing::Test {
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
        auto *plan = new AssertSortedUnique(new Sort(
                new HashDistinct(
                        new Generator(num_rows, 100, SEED, true))));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        delete plan;
    }
};

TEST_F(HashDistinctTest, EmptyTest) {
    auto *plan = new AssertSortedUnique(new Generator(0, 100, SEED));
    plan->run();
    ASSERT_TRUE(plan->isSortedAndUnique());
    ASSERT_EQ(plan->count(), 0);
    delete plan;
}

TEST_F(HashDistinctTest, DetectUnsorted) {
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

TEST_F(HashDistinctTest, DetectDupe) {
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

TEST_F(HashDistinctTest, DedupEmpty) {
    testDedup(0);
}

TEST_F(HashDistinctTest, DedupOne) {
    testDedup(1);
}

TEST_F(HashDistinctTest, DedupTwo) {
    testDedup(2);
}

TEST_F(HashDistinctTest, DedupTiny) {
    testDedup(5);
}

TEST_F(HashDistinctTest, DedupSmall) {
    testDedup(QUEUE_SIZE);
}

TEST_F(HashDistinctTest, DedupSmall2) {
    testDedup(QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(HashDistinctTest, DedupSmallish) {
    testDedup(QUEUE_SIZE * 3);
}

TEST_F(HashDistinctTest, DedupMedium) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS);
}

TEST_F(HashDistinctTest, DedupMediumButSmaller) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS - QUEUE_SIZE / 2);
}

TEST_F(HashDistinctTest, DedupMediumButABitLarger) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE / 2);
}

TEST_F(HashDistinctTest, DedupMediumButABitLarger2) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE * 3 / 2);
}

TEST_F(HashDistinctTest, DedupLarge) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS * 8);
}