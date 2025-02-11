#include "lib/Row.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/GeneratorWithDomains.h"

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

    void testSortDistinctOVC(size_t num_rows) const {
        auto plan = new AssertSortedUnique(new SortDistinctOVC(new GeneratorWithDomains(num_rows, 100, 0, SEED)));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        delete plan;
    }

    void testSortDistinct(size_t num_rows) const {
        auto plan = new AssertSortedUnique(new SortDistinct(new GeneratorWithDomains(num_rows, 100, 0, SEED)));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        delete plan;
    }
};

TEST_F(SortDistinctTest, EmptyTest) {
    auto *plan = new AssertSortedUnique(new SortDistinctOVC(new GeneratorWithDomains(0, 100, 0, SEED)));
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

TEST_F(SortDistinctTest, SortOVCEmpty) {
    testSortDistinctOVC(0);
}

TEST_F(SortDistinctTest, SortOVCOne) {
    testSortDistinctOVC(1);
}

TEST_F(SortDistinctTest, SortOVCTwo) {
    testSortDistinctOVC(2);
}

TEST_F(SortDistinctTest, SortOVCTiny) {
    testSortDistinctOVC(5);
}

TEST_F(SortDistinctTest, SortOVCSmall) {
    testSortDistinctOVC(QUEUE_SIZE);
}

TEST_F(SortDistinctTest, SortOVCSmall2) {
    testSortDistinctOVC(QUEUE_SIZE * 3 / 2);
}

TEST_F(SortDistinctTest, SortOVCSmallish) {
    testSortDistinctOVC(QUEUE_SIZE * 3);
}

TEST_F(SortDistinctTest, SortOVCMedium) {
    testSortDistinctOVC(INITIAL_RUNS * QUEUE_SIZE);
}

TEST_F(SortDistinctTest, SortOVCMediumButSmaller) {
    testSortDistinctOVC(INITIAL_RUNS * QUEUE_SIZE - QUEUE_SIZE / 2);
}

TEST_F(SortDistinctTest, SortOVCMediumButABitLarger) {
    testSortDistinctOVC(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(SortDistinctTest, SortOVCMediumButABitLarger2) {
    testSortDistinctOVC(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE * 3 / 2);
}

TEST_F(SortDistinctTest, SortOVCLarge) {
    testSortDistinctOVC(INITIAL_RUNS * QUEUE_SIZE * 8);
}

TEST_F(SortDistinctTest, SortEmpty) {
    testSortDistinct(0);
}

TEST_F(SortDistinctTest, SortOne) {
    testSortDistinct(1);
}

TEST_F(SortDistinctTest, SortTwo) {
    testSortDistinct(2);
}

TEST_F(SortDistinctTest, SortTiny) {
    testSortDistinct(5);
}

TEST_F(SortDistinctTest, SortSmall) {
    testSortDistinct(QUEUE_SIZE);
}

TEST_F(SortDistinctTest, SortSmall2) {
    testSortDistinct(QUEUE_SIZE * 3 / 2);
}

TEST_F(SortDistinctTest, SortSmallish) {
    testSortDistinct(QUEUE_SIZE * 3);
}

TEST_F(SortDistinctTest, SortMedium) {
    testSortDistinct(INITIAL_RUNS * QUEUE_SIZE);
}

//TEST_F(SortDistinctTest, SortMediumButSmaller) {
//    testSortDistinct(INITIAL_RUNS * QUEUE_SIZE - QUEUE_SIZE / 2);
//}
//
//TEST_F(SortDistinctTest, SortMediumButABitLarger) {
//    testSortDistinct(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE / 2);
//}

TEST_F(SortDistinctTest, SortMediumButABitLarger2) {
    testSortDistinct(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE * 3 / 2);
}

//TEST_F(SortDistinctTest, SortLarge) {
//    testSortDistinct(INITIAL_RUNS * QUEUE_SIZE * 8);
//}