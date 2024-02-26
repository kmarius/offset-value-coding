#include "lib/Row.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/GeneratorWithDomains.h"
#include "lib/iterators/AssertCount.h"
#include "lib/comparators.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/SegmentedSort.h"
#include "lib/iterators/SegmentedSortNoRuns.h"
#include "lib/iterators/RowBuffer.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class SegmentedSortNoRunsTest : public ::testing::Test {
protected:

    const size_t SEED = 1337;

    void SetUp() override {
        log_set_level(LOG_TRACE);
        log_set_quiet(true);
    }

    void TearDown() override {
    }
};

TEST_F(SegmentedSortNoRunsTest, EmptyTest) {
    uint8_t A[ROW_ARITY] = {0};
    auto plan = AssertCount(new SegmentedSortNoRuns(new VectorScan({}), A, 0, A, 0, A, 0));
    plan.run();
    ASSERT_EQ(plan.getCount(), 0);
}

TEST_F(SegmentedSortNoRunsTest, SmallTest) {
    unsigned long domain = 2;
    unsigned long num_rows = 16;

    uint8_t A[ROW_ARITY] = {0};
    uint8_t B[ROW_ARITY] = {1};
    uint8_t ACB[ROW_ARITY] = {0, 2, 1};
    uint8_t CB[ROW_ARITY] = {2, 1};
    int list_length = 1;
    int key_length = 3;

    auto plan = AssertSorted(
            new SegmentedSortNoRuns(new RowBuffer(

                                            new SortPrefix(
                                                    new GeneratorWithDomains(num_rows, {domain, domain, domain}, SEED),
                                                    key_length), num_rows
                                    ),
                                    A, list_length,
                                    B, list_length,
                                    CB, list_length * 2),
            CmpColumnList(ACB, key_length));

    plan.run();
    ASSERT_TRUE(plan.isSorted());
    ASSERT_TRUE(plan.getCount() == num_rows);
}

TEST_F(SegmentedSortNoRunsTest, BigTest) {
    unsigned long domain = 16;
    unsigned long num_rows = 1 << 12;

    uint8_t A[ROW_ARITY] = {0};
    uint8_t B[ROW_ARITY] = {1};
    uint8_t ACB[ROW_ARITY] = {0, 2, 1};
    uint8_t CB[ROW_ARITY] = {2, 1};
    int list_length = 1;
    int key_length = 3;

    auto plan = AssertSorted(
            new SegmentedSortNoRuns<1 << 12>(new RowBuffer(

                                                     new SortPrefix(
                                                             new GeneratorWithDomains(num_rows, {domain, domain, domain}, SEED),
                                                             key_length), num_rows
                                             ),
                                             A, list_length,
                                             B, list_length,
                                             CB, list_length * 2),
            CmpColumnList(ACB, key_length));

    plan.run();
    ASSERT_TRUE(plan.isSorted());
    ASSERT_TRUE(plan.getCount() == num_rows);
}


TEST_F(SegmentedSortNoRunsTest, SmallTestOVC) {
    unsigned long domain = 2;
    unsigned long num_rows = 16;

    uint8_t ABC[ROW_ARITY] = {0, 1, 2};
    uint8_t ACB[ROW_ARITY] = {0, 2, 1};
    auto list_length = 1;
    auto key_length = 3;

    auto plan = AssertSorted(
            new SegmentedSortNoRunsOVC<4096>(
                    new RowBuffer(
                            new SortPrefixOVC(
                                    new GeneratorWithDomains(num_rows, {domain, domain, domain}, SEED),
                                    key_length),
                            num_rows),
                    ABC, list_length, list_length, list_length),
            CmpColumnList(ACB, key_length));

    plan.run();
    ASSERT_TRUE(plan.isSorted());
    ASSERT_TRUE(plan.getCount() == num_rows);
}


TEST_F(SegmentedSortNoRunsTest, BigTestOVC) {
    unsigned long domain = 16;
    unsigned long num_rows = 1 << 12;

    uint8_t ABC[ROW_ARITY] = {0, 1, 2};
    uint8_t ACB[ROW_ARITY] = {0, 2, 1};
    auto list_length = 1;
    auto key_length = 3;

    auto plan = AssertSorted(
            new SegmentedSortNoRunsOVC<4096>(
                    new RowBuffer(
                            new SortPrefixOVC(
                                    new GeneratorWithDomains(num_rows, {domain, domain, domain}, SEED),
                                    key_length),
                            num_rows),
                    ABC, list_length, list_length, list_length),
            CmpColumnList(ACB, key_length));

    plan.run();
    ASSERT_TRUE(plan.isSorted());
    ASSERT_TRUE(plan.getCount() == num_rows);
}

TEST_F(SegmentedSortNoRunsTest, BigTest2ColsOVC) {
    unsigned long domain = 16;
    unsigned long num_rows = 1 << 12;

    uint8_t ABC[ROW_ARITY] = {0, 1, 2, 3, 4, 5};
    uint8_t ACB[ROW_ARITY] = {0, 1, 4, 5, 2, 3};
    auto key_length = 6;
    auto list_length = 2;

    auto plan = AssertSorted(
            new SegmentedSortNoRunsOVC<4096>(
                    new RowBuffer(
                            new SortPrefixOVC(
                                    new GeneratorWithDomains(num_rows, {domain, domain, domain, domain, domain, domain},
                                                             SEED),
                                    key_length),
                            num_rows),
                    ABC, list_length, list_length, list_length),
            CmpColumnListOVC(ACB, key_length));

    plan.run();
    ASSERT_TRUE(plan.isSorted());
    ASSERT_TRUE(plan.getCount() == num_rows);
}