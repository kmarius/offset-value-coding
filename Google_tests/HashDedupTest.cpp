#include "lib/Row.h"
#include "lib/iterators/HashDedup.h"
#include "lib/log.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/Generator.h"
#include "lib/iterators/VectorScan.h"

#include <gtest/gtest.h>

class HashDedupTest : public ::testing::Test {
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
                new HashDedup(
                        new Generator(num_rows, 100, SEED, true))));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        delete plan;
    }
};

TEST_F(HashDedupTest, EmptyTest) {
    auto *plan = new AssertSortedUnique(new Generator(0, 100, SEED));
    plan->run();
    ASSERT_TRUE(plan->isSortedAndUnique());
    ASSERT_EQ(plan->count(), 0);
    delete plan;
}

TEST_F(HashDedupTest, DetectUnsorted) {
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

TEST_F(HashDedupTest, DetectDupe) {
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

TEST_F(HashDedupTest, DedupEmpty) {
    testDedup(0);
}

TEST_F(HashDedupTest, DedupOne) {
    testDedup(1);
}

TEST_F(HashDedupTest, DedupTwo) {
    testDedup(2);
}

TEST_F(HashDedupTest, DedupTiny) {
    testDedup(5);
}

TEST_F(HashDedupTest, DedupSmall) {
    testDedup(QUEUE_SIZE);
}

TEST_F(HashDedupTest, DedupSmall2) {
    testDedup(QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(HashDedupTest, DedupSmallish) {
    testDedup(QUEUE_SIZE * 3);
}

TEST_F(HashDedupTest, DedupMedium) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS);
}

TEST_F(HashDedupTest, DedupMediumButSmaller) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS - QUEUE_SIZE / 2);
}

TEST_F(HashDedupTest, DedupMediumButABitLarger) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE / 2);
}

TEST_F(HashDedupTest, DedupMediumButABitLarger2) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE * 3 / 2);
}

TEST_F(HashDedupTest, DedupLarge) {
    testDedup(QUEUE_SIZE * INITIAL_RUNS * 8);
}