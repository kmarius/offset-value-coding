#include "lib/Row.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/IncreasingRangeGenerator.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/Distinct.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class DistinctTest : public ::testing::Test {
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
        auto *plan = new AssertSortedUnique(
                new Distinct(
                        new Sort(
                                new IncreasingRangeGenerator(num_rows, 100, SEED, true))));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        if (num_rows > 0) {
            ASSERT_TRUE(plan->count() > 0);
        }
        delete plan;
    }

    void testDistinctNoOvc(size_t num_rows) {
        auto *plan = new AssertSortedUnique(
                new DistinctNoOvc(
                        new SortNoOvc(
                                new IncreasingRangeGenerator(num_rows, 100, SEED, true))));
        plan->run();
        ASSERT_TRUE(plan->isSortedAndUnique());
        delete plan;
    }
};

TEST_F(DistinctTest, EmptyTest) {
    auto *plan = new AssertSortedUnique(new VectorScan({}));
    plan->run();
    ASSERT_TRUE(plan->isSortedAndUnique());
    ASSERT_EQ(plan->count(), 0);
    delete plan;
}

TEST_F(DistinctTest, DetectUnsorted) {
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

TEST_F(DistinctTest, DetectDupe) {
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

TEST_F(DistinctTest, DistinctEmpty) {
    testDistinct(0);
}

TEST_F(DistinctTest, DistinctOne) {
    testDistinct(1);
}

TEST_F(DistinctTest, DistinctTwo) {
    testDistinct(2);
}

TEST_F(DistinctTest, DistinctTiny) {
    testDistinct(5);
}

TEST_F(DistinctTest, DistinctSmall) {
    testDistinct(QUEUE_SIZE);
}

TEST_F(DistinctTest, DistinctSmall2) {
    testDistinct(QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(DistinctTest, DistinctSmallish) {
    testDistinct(QUEUE_SIZE * 3);
}

TEST_F(DistinctTest, DistinctMedium) {
    testDistinct(QUEUE_SIZE * INITIAL_RUNS);
}

TEST_F(DistinctTest, DistinctMediumButSmaller) {
    testDistinct(QUEUE_SIZE * INITIAL_RUNS - QUEUE_SIZE / 2);
}

TEST_F(DistinctTest, DistinctMediumButABitLarger) {
    testDistinct(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE / 2);
}

TEST_F(DistinctTest, DistinctMediumButABitLarger2) {
    testDistinct(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE * 3 / 2);
}

TEST_F(DistinctTest, DistinctLarge) {
    testDistinct(QUEUE_SIZE * INITIAL_RUNS * 8);
}

TEST_F(DistinctTest, DistinctNoOvcEmpty) {
    testDistinctNoOvc(0);
}

TEST_F(DistinctTest, DistinctNoOvcOne) {
    testDistinctNoOvc(1);
}

TEST_F(DistinctTest, DistinctNoOvcTwo) {
    testDistinctNoOvc(2);
}

TEST_F(DistinctTest, DistinctNoOvcTiny) {
    testDistinctNoOvc(5);
}

TEST_F(DistinctTest, DistinctNoOvcSmall) {
    testDistinctNoOvc(QUEUE_SIZE);
}

TEST_F(DistinctTest, DistinctNoOvcSmall2) {
    testDistinctNoOvc(QUEUE_SIZE + QUEUE_SIZE / 2);
}

TEST_F(DistinctTest, DistinctNoOvcSmallish) {
    testDistinctNoOvc(QUEUE_SIZE * 3);
}

TEST_F(DistinctTest, DistinctNoOvcMedium) {
    testDistinctNoOvc(QUEUE_SIZE * INITIAL_RUNS);
}

//TEST_F(DistinctTest, DistinctNoOvcMediumButSmaller) {
//    testDistinctNoOvc(QUEUE_SIZE * INITIAL_RUNS - QUEUE_SIZE / 2);
//}
//
//TEST_F(DistinctTest, DistinctNoOvcMediumButABitLarger) {
//    testDistinctNoOvc(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE / 2);
//}
//
TEST_F(DistinctTest, DistinctNoOvcMediumButABitLarger2) {
    testDistinctNoOvc(QUEUE_SIZE * INITIAL_RUNS + QUEUE_SIZE * 3 / 2);
}
//
//TEST_F(DistinctTest, DistinctNoOvcLarge) {
//    testDistinctNoOvc(QUEUE_SIZE * INITIAL_RUNS * 8);
//}