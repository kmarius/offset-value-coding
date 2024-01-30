#include "lib/iterators/HashDistinct.h"
#include "lib/log.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/IncreasingRangeGenerator.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/GeneratorWithDomains.h"

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

    void testDistinct(size_t num_rows) {
        auto *plan = new AssertSortedUnique(new SortOVC(
                new HashDistinct(
                        new GeneratorWithDomains(num_rows, 100, 0, SEED))));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        delete plan;
    }
};

TEST_F(HashDistinctTest, EmptyTest) {
    auto *plan = new AssertSortedUnique(new GeneratorWithDomains(0, 100, 0, SEED));
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

TEST_F(HashDistinctTest, DistinctEmpty) {
    testDistinct(0);
}

TEST_F(HashDistinctTest, DistinctOne) {
    testDistinct(1);
}

TEST_F(HashDistinctTest, DistinctTwo) {
    testDistinct(2);
}

TEST_F(HashDistinctTest, DistinctTiny) {
    testDistinct(5);
}

TEST_F(HashDistinctTest, DistinctSmall) {
    testDistinct(QUEUE_SIZE);
}

TEST_F(HashDistinctTest, DistinctSmall2) {
    testDistinct(QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(HashDistinctTest, DistinctSmallish) {
    testDistinct(QUEUE_SIZE * 3);
}

TEST_F(HashDistinctTest, DistinctMedium) {
    testDistinct(QUEUE_SIZE * INITIAL_RUNS);
}

TEST_F(HashDistinctTest, DistinctMediumButSmaller) {
    testDistinct(QUEUE_SIZE * INITIAL_RUNS - QUEUE_SIZE / 2);
}

TEST_F(HashDistinctTest, DistinctMediumButABitLarger) {
    testDistinct(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE / 2);
}

TEST_F(HashDistinctTest, DistinctMediumButABitLarger2) {
    testDistinct(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE * 3 / 2);
}

TEST_F(HashDistinctTest, DistinctLarge) {
    testDistinct(QUEUE_SIZE * INITIAL_RUNS * 8);
}