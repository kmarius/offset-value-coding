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
    }

    void TearDown() override {

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
    auto *plan = new AssertSorted(
            new Sort(
                    new Generator(0, 100, SEED
                    )));
    plan->run();
    ASSERT_TRUE(plan->isSorted());
    ASSERT_EQ(plan->count(), 0);
    delete plan;
}

TEST_F(IteratorTest, SortOne) {
    size_t num_rows = 1;
    auto *plan = new AssertSorted(
            new Sort(
                    new Generator(num_rows, 100, SEED
                    )));
    plan->run();
    ASSERT_TRUE(plan->isSorted());
    ASSERT_EQ(plan->count(), num_rows);
    delete plan;
}

TEST_F(IteratorTest, SortTwo) {
    size_t num_rows = 2;
    auto *plan = new AssertSorted(
            new Sort(
                    new Generator(num_rows, 100, SEED
                    )));
    plan->run();
    ASSERT_TRUE(plan->isSorted());
    ASSERT_EQ(plan->count(), num_rows);
    delete plan;
}

TEST_F(IteratorTest, SortTiny) {
    size_t num_rows = 5;
    auto *plan = new AssertSorted(
            new Sort(
                    new Generator(num_rows, 100, SEED
                    )));
    plan->run();
    ASSERT_TRUE(plan->isSorted());
    ASSERT_EQ(plan->count(), num_rows);
    delete plan;
}

TEST_F(IteratorTest, SortSmall) {
    size_t num_rows = QUEUE_SIZE;
    auto *plan = new AssertSorted(
            new Sort(
                    new Generator(num_rows, 100, SEED
                    )));
    plan->run();
    ASSERT_TRUE(plan->isSorted());
    ASSERT_EQ(plan->count(), num_rows);
    delete plan;
}

TEST_F(IteratorTest, SortSmallish) {
    size_t num_rows = QUEUE_SIZE * 3;
    auto *plan = new AssertSorted(
            new Sort(
                    new Generator(num_rows, 100, SEED
                    )));
    plan->run();
    ASSERT_TRUE(plan->isSorted());
    ASSERT_EQ(plan->count(), num_rows);
    delete plan;
}

TEST_F(IteratorTest, SortMedium) {
    size_t num_rows = QUEUE_SIZE * QUEUE_SIZE;
    auto *plan = new AssertSorted(
            new Sort(
                    new Generator(num_rows, 100, SEED
                    )));
    plan->run();
    ASSERT_TRUE(plan->isSorted());
    ASSERT_EQ(plan->count(), num_rows);
    delete plan;
}

TEST_F(IteratorTest, SortLarge) {
    size_t num_rows = QUEUE_SIZE * QUEUE_SIZE * (QUEUE_SIZE - 3);
    auto *plan = new AssertSorted(
            new Sort(
                    new Generator(num_rows, 100, SEED
                    )));
    plan->run();
    ASSERT_TRUE(plan->isSorted());
    ASSERT_EQ(plan->count(), num_rows);
    delete plan;
}

TEST_F(IteratorTest, SortLarger) {
    size_t num_rows = QUEUE_SIZE * QUEUE_SIZE * (QUEUE_SIZE - 3) * 2;
    auto *plan = new AssertSorted(
            new Sort(
                    new Generator(num_rows, 100, SEED
                    )));
    plan->run();
    ASSERT_TRUE(plan->isSorted());
    ASSERT_EQ(plan->count(), num_rows);
    delete plan;
}