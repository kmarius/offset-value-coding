#include "lib/Row.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/AssertEqual.h"
#include "lib/iterators/RowGeneratorWithDomains.h"
#include "lib/comparators.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class SortTest : public ::testing::Test {
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

    void testSortOVC(size_t num_rows) {
        auto gen = new RowGeneratorWithDomains(num_rows, 100, 0, SEED);
        auto rows = RowGeneratorWithDomains(num_rows, 100, 0, SEED).collect();
        auto sorted = new AssertSorted(new SortOVC(gen));
        Cmp cmp;
        std::sort(rows.begin(), rows.end(),
                  [cmp](const Row &a, const Row &b) -> bool {
                      return cmp(a, b) < 0;
                  });
        auto *plan = new AssertEqual(sorted, new VectorScan(rows));
        plan->run(true);
        ASSERT_TRUE(sorted->isSorted());
        ASSERT_EQ(sorted->getCount(), num_rows);
        ASSERT_TRUE(plan->isEqual());
        delete plan;
    }

    void testSort(size_t num_rows) {
        auto gen = new RowGeneratorWithDomains(num_rows, 100, 0, SEED);
        auto rows = RowGeneratorWithDomains(num_rows, 100, 0, SEED).collect();
        auto sorted = new AssertSorted(new Sort(gen));
        Cmp cmp;
        std::sort(rows.begin(), rows.end(),
                  [cmp](const Row &a, const Row &b) -> bool {
                      return cmp(a, b) < 0;
                  });
        auto *plan = new AssertEqual(sorted, new VectorScan(rows));
        plan->run();
        ASSERT_TRUE(sorted->isSorted());
        ASSERT_EQ(sorted->getCount(), num_rows);
        ASSERT_TRUE(plan->isEqual());
        delete plan;
    }
};

TEST_F(SortTest, EmptyTest) {
    auto *plan = new AssertSorted(new VectorScan({}));
    plan->run();
    ASSERT_TRUE(plan->isSorted());
    ASSERT_EQ(plan->getCount(), 0);
    delete plan;
}

TEST_F(SortTest, DetectUnsorted) {
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

TEST_F(SortTest, SortOVCEmpty) {
    testSortOVC(0);
}

TEST_F(SortTest, SortOVCOne) {
    testSortOVC(1);
}

TEST_F(SortTest, SortOVCTwo) {
    testSortOVC(2);
}

TEST_F(SortTest, SortOVCTiny) {
    testSortOVC(5);
}

TEST_F(SortTest, SortOVCSmall) {
    testSortOVC(QUEUE_SIZE);
}

TEST_F(SortTest, SortOVCSmall2) {
    testSortOVC(QUEUE_SIZE * 3 / 2);
}

TEST_F(SortTest, SortOVCSmallish) {
    testSortOVC(QUEUE_SIZE * 3);
}

TEST_F(SortTest, SortOVCSmallish2) {
    testSortOVC(QUEUE_SIZE * 4);
}

TEST_F(SortTest, SortOVCSmallish3) {
    testSortOVC(QUEUE_SIZE * 5);
}

TEST_F(SortTest, SortOVCSmallish4) {
    testSortOVC(QUEUE_SIZE * 6);
}

TEST_F(SortTest, SortOVCMedium) {
    testSortOVC(INITIAL_RUNS * QUEUE_SIZE);
}

TEST_F(SortTest, SortOVCMediumButSmaller) {
    testSortOVC(INITIAL_RUNS * QUEUE_SIZE - QUEUE_SIZE / 2);
}

TEST_F(SortTest, SortOVCMediumButABitLarger) {
    testSortOVC(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(SortTest, SortOVCMediumButABitLarger2) {
    testSortOVC(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE * 3 / 2);
}

TEST_F(SortTest, SortOVCLarge) {
    testSortOVC(INITIAL_RUNS * QUEUE_SIZE * 8);
}


TEST_F(SortTest, SortEmpty) {
    testSort(0);
}

TEST_F(SortTest, SortOne) {
    testSort(1);
}

TEST_F(SortTest, SortTwo) {
    testSort(2);
}

TEST_F(SortTest, SortTiny) {
    testSort(5);
}

TEST_F(SortTest, SortSmall) {
    testSort(QUEUE_SIZE);
}

TEST_F(SortTest, SortSmall2) {
    testSort(QUEUE_SIZE * 3 / 2);
}

TEST_F(SortTest, SortSmallish) {
    testSort(QUEUE_SIZE * 3);
}

TEST_F(SortTest, SortSmallish2) {
    testSort(QUEUE_SIZE * 4);
}

TEST_F(SortTest, SortSmallish3) {
    testSort(QUEUE_SIZE * 5);
}

TEST_F(SortTest, SortSmallish4) {
    testSort(QUEUE_SIZE * 6);
}

TEST_F(SortTest, SortMedium) {
    testSortOVC(INITIAL_RUNS * QUEUE_SIZE);
}

//TEST_F(SortTest, SortMediumButSmaller) {
//    testInSortDistinct(INITIAL_RUNS * QUEUE_SIZE - QUEUE_SIZE / 2);
//}
//
//TEST_F(SortTest, SortMediumButABitLarger) {
//    testInSortDistinct(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE / 2);
//}
//
TEST_F(SortTest, SortMediumButABitLarger2) {
    testSortOVC(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE * 3 / 2);
}
//
//TEST_F(SortTest, SortLarge) {
//    testInSortDistinct(INITIAL_RUNS * QUEUE_SIZE * 8);
//}