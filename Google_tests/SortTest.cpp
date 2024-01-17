#include "lib/Row.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/IncreasingRangeGenerator.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/AssertEqual.h"
#include "lib/iterators/RowGeneratorWithDomains.h"

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

    void testSorted(size_t num_rows) {
        auto gen = new RowGeneratorWithDomains(num_rows, 100, 0, SEED);
        auto rows = RowGeneratorWithDomains(num_rows, 100, 0, SEED).collect();
        auto sorted = new AssertSorted(new Sort(gen));
        CmpNoOVC cmp;
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

    void testSortedNoOvc(size_t num_rows) {
        auto gen = new RowGeneratorWithDomains(num_rows, 100, 0, SEED);
        auto rows = RowGeneratorWithDomains(num_rows, 100, 0, SEED).collect();
        auto sorted = new AssertSorted(new SortNoOVC(gen));
        CmpNoOVC cmp;
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

TEST_F(SortTest, SortEmpty) {
    testSorted(0);
}

TEST_F(SortTest, SortOne) {
    testSorted(1);
}

TEST_F(SortTest, SortTwo) {
    testSorted(2);
}

TEST_F(SortTest, SortTiny) {
    testSorted(5);
}

TEST_F(SortTest, SortSmall) {
    testSorted(QUEUE_SIZE);
}

TEST_F(SortTest, SortSmall2) {
    testSorted(QUEUE_SIZE * 3 / 2);
}

TEST_F(SortTest, SortSmallish) {
    testSorted(QUEUE_SIZE * 3);
}

TEST_F(SortTest, SortSmallish2) {
    testSorted(QUEUE_SIZE * 4);
}

TEST_F(SortTest, SortSmallish3) {
    testSorted(QUEUE_SIZE * 5);
}

TEST_F(SortTest, SortSmallish4) {
    testSorted(QUEUE_SIZE * 6);
}

TEST_F(SortTest, SortMedium) {
    testSorted(INITIAL_RUNS * QUEUE_SIZE);
}

TEST_F(SortTest, SortMediumButSmaller) {
    testSorted(INITIAL_RUNS * QUEUE_SIZE - QUEUE_SIZE / 2);
}

TEST_F(SortTest, SortMediumButABitLarger) {
    testSorted(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(SortTest, SortMediumButABitLarger2) {
    testSorted(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE * 3 / 2);
}

TEST_F(SortTest, SortLarge) {
    testSorted(INITIAL_RUNS * QUEUE_SIZE * 8);
}


TEST_F(SortTest, SortEmptyNoOvc) {
    testSortedNoOvc(0);
}

TEST_F(SortTest, SortOneNoOvc) {
    testSortedNoOvc(1);
}

TEST_F(SortTest, SortTwoNoOvc) {
    testSortedNoOvc(2);
}

TEST_F(SortTest, SortTinyNoOvc) {
    testSortedNoOvc(5);
}

TEST_F(SortTest, SortSmallNoOvc) {
    testSortedNoOvc(QUEUE_SIZE);
}

TEST_F(SortTest, SortSmallNoOvc2) {
    testSortedNoOvc(QUEUE_SIZE * 3 / 2);
}

TEST_F(SortTest, SortSmallishNoOvc) {
    testSortedNoOvc(QUEUE_SIZE * 3);
}

TEST_F(SortTest, SortSmallishNoOvc2) {
    testSortedNoOvc(QUEUE_SIZE * 4);
}

TEST_F(SortTest, SortSmallishNoOvc3) {
    testSortedNoOvc(QUEUE_SIZE * 5);
}

TEST_F(SortTest, SortSmallishNoOvc4) {
    testSortedNoOvc(QUEUE_SIZE * 6);
}

TEST_F(SortTest, SortMediumNoOvc) {
    testSorted(INITIAL_RUNS * QUEUE_SIZE);
}

//TEST_F(SortTest, SortMediumButSmallerNoOvc) {
//    testInSortDistinct(INITIAL_RUNS * QUEUE_SIZE - QUEUE_SIZE / 2);
//}
//
//TEST_F(SortTest, SortMediumButABitLargerNoOvc) {
//    testInSortDistinct(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE / 2);
//}
//
TEST_F(SortTest, SortMediumButABitLargerNoOvc2) {
    testSorted(INITIAL_RUNS * QUEUE_SIZE + QUEUE_SIZE * 3 / 2);
}
//
//TEST_F(SortTest, SortLargeNoOvc) {
//    testInSortDistinct(INITIAL_RUNS * QUEUE_SIZE * 8);
//}