#include "lib/Row.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/SegmentedSort.h"
#include "lib/iterators/AssertEqual.h"
#include "lib/iterators/RowGeneratorWithDomains.h"
#include "lib/iterators/AssertCount.h"
#include "lib/comparators.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class SegmentedSortTest : public ::testing::Test {
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
};

TEST_F(SegmentedSortTest, EmptyTest) {
    auto *plan = new AssertCount(new VectorScan({}));
    plan->run();
    ASSERT_EQ(plan->getCount(), 0);
    delete plan;
}

TEST_F(SegmentedSortTest, TinyTest0) {
    auto vec = new VectorScan({
                                      {0, 0, {0}}
                              });
    auto wanted = new VectorScan({
                                         {0, 0, {0}}
                                 });

    auto plan = AssertEqual(
            new SegmentedSortBase<false, comparators::CmpPrefix, comparators::EqPrefix>(
                    vec, comparators::CmpPrefix(2), comparators::EqPrefix(1)
            ),
            wanted
    );

    plan.run();
    ASSERT_TRUE(plan.isEqual());
}

TEST_F(SegmentedSortTest, TinyTest1) {
    auto vec = new VectorScan({
                                      {0, 0, {0, 2}},
                                      {0, 0, {0, 1}}
                              });
    auto wanted = new VectorScan({
                                         {0, 0, {0, 1}},
                                         {0, 0, {0, 2}}
                                 });

    auto plan = AssertEqual(
            new SegmentedSort(
                    vec, comparators::CmpColumnList({1}), comparators::EqColumnList({0})
            ),
            wanted
    );

    plan.run();
    ASSERT_TRUE(plan.isEqual());
}


TEST_F(SegmentedSortTest, TinyTest2) {
    auto vec = new VectorScan({
                                      {0, 0, {1, 2}},
                                      {0, 0, {1, 1}},

                                      {0, 0, {0, 2}},
                                      {0, 0, {0, 1}}
                              });
    auto wanted = new VectorScan({
                                         {0, 0, {1, 1}},
                                         {0, 0, {1, 2}},

                                         {0, 0, {0, 1}},
                                         {0, 0, {0, 2}}
                                 });

    auto plan = AssertEqual(
            new SegmentedSort(
                    vec, comparators::CmpColumnList({1}), comparators::EqColumnList({0})
            ),
            wanted
    );

    plan.run();
    ASSERT_TRUE(plan.isEqual());
}

TEST_F(SegmentedSortTest, TinyTest3) {
    auto vec = new VectorScan({
                                       {0, 2, {1, 2}},
                                       {0, 3, {1, 3}},

                                       {0, 1, {0, 1}},
                                       {0, 1, {0, 2}},
                                       {0, 0, {0, 3}},
                                       {0, 1, {0, 4}},
                                       {0, 1, {0, 5}},
                                       {0, 1, {0, 6}},
                                       {0, 1, {0, 7}},
                                       {0, 1, {0, 8}},

                                       {0, 1, {3, 9}},

                               });

    auto wanted = new VectorScan({
                                      {0, 2, {1, 2}},
                                      {0, 3, {1, 3}},

                                      {0, 1, {0, 1}},
                                      {0, 1, {0, 2}},
                                      {0, 0, {0, 3}},
                                      {0, 1, {0, 4}},
                                      {0, 1, {0, 5}},
                                      {0, 1, {0, 6}},
                                      {0, 1, {0, 7}},
                                      {0, 1, {0, 8}},

                                      {0, 1, {3, 9}},
                              });

    auto plan = AssertEqual(
            new SegmentedSort(
                    vec, comparators::CmpColumnList({1}), comparators::EqColumnList({0})
            ),
            wanted
    );

    plan.run();
    ASSERT_TRUE(plan.isEqual());
}
