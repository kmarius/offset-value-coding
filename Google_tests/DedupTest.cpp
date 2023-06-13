#include "lib/Row.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/Generator.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/Dedup.h"

#include <gtest/gtest.h>

class DedupTest : public ::testing::Test {
protected:

    const size_t QUEUE_SIZE = QUEUE_CAPACITY;
    const size_t SEED = 1337;

    void SetUp() override {
        log_set_quiet(true);
        log_set_level(LOG_ERROR);
    }

    void TearDown() override {

    }

    void testDedup(size_t num_rows) {
        auto *plan = new AssertSortedUnique(
                new Dedup(
                        new Sort(
                                new Generator(num_rows, 100, SEED, true))));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        delete plan;
    }
};

TEST_F(DedupTest, EmptyTest) {
    auto *plan = new AssertSortedUnique(new Generator(0, 100, SEED));
    plan->run();
    ASSERT_TRUE(plan->isSortedAndUnique());
    ASSERT_EQ(plan->count(), 0);
    delete plan;
}

TEST_F(DedupTest, DetectUnsorted) {
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

TEST_F(DedupTest, DetectDupe) {
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

TEST_F(DedupTest, DedupEmpty) {
    testDedup(0);
}

TEST_F(DedupTest, DedupOne) {
    testDedup(1);
}

TEST_F(DedupTest, DedupTwo) {
    testDedup(2);
}

TEST_F(DedupTest, DedupTiny) {
    testDedup(5);
}

TEST_F(DedupTest, DedupSmall) {
    testDedup(QUEUE_SIZE);
}

TEST_F(DedupTest, DedupSmall2) {
    testDedup(QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(DedupTest, DedupSmallish) {
    testDedup(QUEUE_SIZE * 3);
}

TEST_F(DedupTest, DedupMedium) {
    testDedup(QUEUE_SIZE * (QUEUE_SIZE - 3));
}

TEST_F(DedupTest, DedupMediumButSmaller) {
    testDedup(QUEUE_SIZE * (QUEUE_SIZE - 3) - QUEUE_SIZE / 2);
}

TEST_F(DedupTest, DedupMediumButABitLarger) {
    testDedup(QUEUE_SIZE * QUEUE_SIZE * (QUEUE_SIZE - 3) + QUEUE_SIZE / 2);
}

TEST_F(DedupTest, DedupMediumButABitLarger2) {
    testDedup(QUEUE_SIZE * QUEUE_SIZE * (QUEUE_SIZE - 3) + QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(DedupTest, DedupLarge) {
    testDedup(QUEUE_SIZE * QUEUE_SIZE * (QUEUE_SIZE - 3));
}

TEST_F(DedupTest, DedupLarger) {
    testDedup(QUEUE_SIZE * QUEUE_SIZE * (QUEUE_SIZE - 3) * 17 + 1337);
}