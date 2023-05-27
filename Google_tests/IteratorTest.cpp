#include "lib/Row.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/Generator.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/Sort.h"

#include <gtest/gtest.h>

class IteratorTest : public ::testing::Test {
protected:

    const size_t QUEUE_SIZE = QUEUE_CAPACITY;
    const size_t SEED = 1337;

    void SetUp() override {
        log_set_quiet(true);
        log_set_level(LOG_INFO);
    }

    void TearDown() override {

    }

    void testSorted(size_t num_rows) {
        auto *plan = new AssertSorted(
                new Sort(
                        new Generator(num_rows, 100, SEED
                        )));
        plan->run();
        ASSERT_TRUE(plan->isSorted());
        ASSERT_EQ(plan->count(), num_rows);
        delete plan;
    }

};

TEST_F(IteratorTest, EmptyTest) {
    auto *plan = new AssertSorted(new Generator(0, 100, SEED));
    plan->run();
    ASSERT_TRUE(plan->isSorted());
    ASSERT_EQ(plan->count(), 0);
    delete plan;
}

TEST_F(IteratorTest, DetectUnsorted) {
    auto *plan = new AssertSorted(new VectorScan(
            {
                    {0, 0, {1, 0, 0, 0}},
                    {0, 0, {0, 0, 0, 0}}
            }
    ));
    plan->run();
    ASSERT_FALSE(plan->isSorted());
    delete plan;
}

TEST_F(IteratorTest, SortEmpty) {
    testSorted(0);
}

TEST_F(IteratorTest, SortOne) {
    testSorted(1);
}

TEST_F(IteratorTest, SortTwo) {
    testSorted(2);
}

TEST_F(IteratorTest, SortTiny) {
    testSorted(5);
}

TEST_F(IteratorTest, SortSmall) {
    testSorted(QUEUE_SIZE);
}

TEST_F(IteratorTest, SortSmall2) {
    testSorted(QUEUE_SIZE + QUEUE_SIZE/2);
}

TEST_F(IteratorTest, SortSmallish) {
    testSorted(QUEUE_SIZE * 3);
}

TEST_F(IteratorTest, SortMedium) {
    testSorted(QUEUE_SIZE * (QUEUE_SIZE - 3));
}

TEST_F(IteratorTest, SortMediumButSmaller) {
    testSorted(QUEUE_SIZE * (QUEUE_SIZE - 3) - QUEUE_SIZE/2);
}

TEST_F(IteratorTest, SortMediumButABitLarger) {
    testSorted(QUEUE_SIZE * QUEUE_SIZE * (QUEUE_SIZE - 3) + QUEUE_SIZE/2);
}

TEST_F(IteratorTest, SortMediumButABitLarger2) {
    testSorted(QUEUE_SIZE * QUEUE_SIZE * (QUEUE_SIZE - 3) + QUEUE_SIZE + QUEUE_SIZE/2);
}

TEST_F(IteratorTest, SortLarge) {
    testSorted(QUEUE_SIZE * QUEUE_SIZE * (QUEUE_SIZE - 3));
}

TEST_F(IteratorTest, SortLarger) {
    testSorted(QUEUE_SIZE * QUEUE_SIZE * (QUEUE_SIZE - 3) * 17 + 1337);
}
