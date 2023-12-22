#include "lib/Row.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/IncreasingRangeGenerator.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/InStreamDistinct.h"
#include "lib/iterators/RowGeneratorWithDomains.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class InStreamDistinctTest : public ::testing::Test {
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

    void testInStreamDistinct(size_t num_rows) {
        auto *plan = new AssertSortedUnique(
                new InStreamDistinct(
                        new Sort(
                                new RowGeneratorWithDomains(num_rows, 100, 0, SEED))));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        if (num_rows > 0) {
            ASSERT_TRUE(plan->count() > 0);
        }
        delete plan;
    }

    void testInStreamDistinctNoOvc(size_t num_rows) {
        auto *plan = new AssertSortedUnique(
                new InStreamDistinctNoOvc(
                        new SortNoOvc(
                                new RowGeneratorWithDomains(num_rows, 100, 0, SEED))));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        delete plan;
    }
};

TEST_F(InStreamDistinctTest, EmptyTest) {
    auto *plan = new AssertSortedUnique(new VectorScan({}));
    plan->run();
    ASSERT_TRUE(plan->isSortedAndUnique());
    ASSERT_EQ(plan->count(), 0);
    delete plan;
}

TEST_F(InStreamDistinctTest, DetectUnsorted) {
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

TEST_F(InStreamDistinctTest, DetectDupe) {
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

TEST_F(InStreamDistinctTest, DistinctEmpty) {
    testInStreamDistinct(0);
}

TEST_F(InStreamDistinctTest, DistinctOne) {
    testInStreamDistinct(1);
}

TEST_F(InStreamDistinctTest, DistinctTwo) {
    testInStreamDistinct(2);
}

TEST_F(InStreamDistinctTest, DistinctTiny) {
    testInStreamDistinct(5);
}

TEST_F(InStreamDistinctTest, DistinctSmall) {
    testInStreamDistinct(QUEUE_SIZE);
}

TEST_F(InStreamDistinctTest, DistinctSmall2) {
    testInStreamDistinct(QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(InStreamDistinctTest, DistinctSmallish) {
    testInStreamDistinct(QUEUE_SIZE * 3);
}

TEST_F(InStreamDistinctTest, DistinctMedium) {
    testInStreamDistinct(QUEUE_SIZE * INITIAL_RUNS);
}

TEST_F(InStreamDistinctTest, DistinctMediumButSmaller) {
    testInStreamDistinct(QUEUE_SIZE * INITIAL_RUNS - QUEUE_SIZE / 2);
}

TEST_F(InStreamDistinctTest, DistinctMediumButABitLarger) {
    testInStreamDistinct(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE / 2);
}

TEST_F(InStreamDistinctTest, DistinctMediumButABitLarger2) {
    testInStreamDistinct(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE * 3 / 2);
}

TEST_F(InStreamDistinctTest, DistinctLarge) {
    testInStreamDistinct(QUEUE_SIZE * INITIAL_RUNS * 8);
}

TEST_F(InStreamDistinctTest, DistinctNoOvcEmpty) {
    testInStreamDistinctNoOvc(0);
}

TEST_F(InStreamDistinctTest, DistinctNoOvcOne) {
    testInStreamDistinctNoOvc(1);
}

TEST_F(InStreamDistinctTest, DistinctNoOvcTwo) {
    testInStreamDistinctNoOvc(2);
}

TEST_F(InStreamDistinctTest, DistinctNoOvcTiny) {
    testInStreamDistinctNoOvc(5);
}

TEST_F(InStreamDistinctTest, DistinctNoOvcSmall) {
    testInStreamDistinctNoOvc(QUEUE_SIZE);
}

TEST_F(InStreamDistinctTest, DistinctNoOvcSmall2) {
    testInStreamDistinctNoOvc(QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(InStreamDistinctTest, DistinctNoOvcSmallish) {
    testInStreamDistinctNoOvc(QUEUE_SIZE * 3);
}

TEST_F(InStreamDistinctTest, DistinctNoOvcMedium) {
    testInStreamDistinctNoOvc(QUEUE_SIZE * INITIAL_RUNS);
}

//TEST_F(InStreamDistinctTest, DistinctNoOvcMediumButSmaller) {
//    testInStreamDistinctNoOvc(QUEUE_SIZE * INITIAL_RUNS - QUEUE_SIZE / 2);
//}
//
//TEST_F(InStreamDistinctTest, DistinctNoOvcMediumButABitLarger) {
//    testInStreamDistinctNoOvc(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE / 2);
//}
//
TEST_F(InStreamDistinctTest, DistinctNoOvcMediumButABitLarger2) {
    testInStreamDistinctNoOvc(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE * 3 / 2);
}
//
//TEST_F(InStreamDistinctTest, DistinctNoOvcLarge) {
//    testInStreamDistinctNoOvc(QUEUE_SIZE * INITIAL_RUNS * 8);
//}