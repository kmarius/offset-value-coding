#include "lib/Row.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/SegmentedSort.h"
#include "lib/iterators/AssertEqual.h"
#include "lib/iterators/GeneratorWithDomains.h"
#include "lib/iterators/AssertCount.h"
#include "lib/comparators.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/RowBuffer.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class SegmentedSortTest : public ::testing::Test {
protected:

    const size_t SEED = 1337;

    void SetUp() override {
        log_set_level(LOG_TRACE);
        log_set_quiet(true);
    }

    void TearDown() override {
    }
};

TEST_F(SegmentedSortTest, EmptyTest) {
    auto plan = AssertCount(new VectorScan({}));
    plan.run();
    ASSERT_EQ(plan.getCount(), 0);
}

TEST_F(SegmentedSortTest, SmallTest) {
    unsigned long domain = 2;
    unsigned long num_rows = 16;

    uint8_t A[ROW_ARITY] = {0};
    uint8_t B[ROW_ARITY] = {1};
    uint8_t ACB[ROW_ARITY] = {0, 2, 1};
    uint8_t CB[ROW_ARITY] = {2, 1};
    int list_length = 1;
    int key_length = 3;

    auto plan = AssertSorted(
            new SegmentedSort(
                    new SortPrefix(
                            new GeneratorWithDomains(num_rows, {domain, domain, domain}, SEED),
                            key_length),
                    A, list_length,
                    B, list_length,
                    CB, list_length * 2),
            CmpColumnList(ACB, key_length));

    plan.run(true);
    ASSERT_TRUE(plan.isSorted());
    ASSERT_TRUE(plan.getCount() == num_rows);
}

TEST_F(SegmentedSortTest, BigTest) {
    unsigned long domain = 16;
    unsigned long num_rows = 1 << 12;

    uint8_t A[ROW_ARITY] = {0, 1};
    uint8_t B[ROW_ARITY] = {2, 3};
    uint8_t ACB[ROW_ARITY] = {0, 1, 4, 5, 2, 3};
    uint8_t CB[ROW_ARITY] = {4, 5, 2, 3};
    int list_length = 2;
    int key_length = 6;

    auto plan = AssertSorted(
            new SegmentedSort(
                    new RowBuffer(
                            new SortPrefix(
                                    new GeneratorWithDomains(
                                            num_rows,
                                            {domain, domain, domain, domain, domain, domain},
                                            SEED),
                                    key_length),
                            num_rows),
                    A, list_length,
                    B, list_length,
                    CB, list_length * 2),
            CmpColumnList(ACB, key_length));

    plan.run(true);
    ASSERT_TRUE(plan.isSorted());
    ASSERT_TRUE(plan.getCount() == num_rows);
}

TEST_F(SegmentedSortTest, BigTest2) {
    unsigned long domain = 16;
    unsigned long num_rows = 1 << 12;

    uint8_t A[ROW_ARITY] = {0};
    uint8_t B[ROW_ARITY] = {1};
    uint8_t ACB[ROW_ARITY] = {0, 2, 1};
    uint8_t CB[ROW_ARITY] = {2, 1};

    int list_length = 1;
    int key_length = 3;

    auto plan = AssertSorted(
            new SegmentedSort(
                    new RowBuffer(
                            new SortPrefix(
                                    new GeneratorWithDomains(num_rows, {domain, domain, domain}, SEED),
                                    key_length),
                            num_rows),
                    A, list_length,
                    B, list_length,
                    CB, list_length * 2),
            CmpColumnList(ACB, key_length));

    plan.run();
    ASSERT_TRUE(plan.isSorted());
    ASSERT_TRUE(plan.getCount() == num_rows);
}

TEST_F(SegmentedSortTest, BigTest2OVC) {
    unsigned long domain = 8;
    unsigned long num_rows = 1 << 12;

    uint8_t ABC[ROW_ARITY] = {0, 1, 2};
    uint8_t ACB[ROW_ARITY] = {0, 2, 1};
    auto list_length = 1;
    auto key_length = 3;

    auto plan = AssertSorted(
            new SegmentedSortOVC(
                    new RowBuffer(
                            new SortPrefixOVC(
                                    new GeneratorWithDomains(num_rows, {domain, domain, domain}, SEED),
                                    key_length),
                            num_rows),
                    ABC, list_length, list_length, list_length),
            CmpColumnListOVC(ACB, key_length));

    plan.run();
    ASSERT_TRUE(plan.isSorted());
    ASSERT_TRUE(plan.getCount() == num_rows);
}


TEST_F(SegmentedSortTest, BigTest3OVC) {
    unsigned long domain = 16;
    unsigned long num_rows = 1 << 10;

    uint8_t ABC[ROW_ARITY] = {0, 1, 2, 3, 4, 5};
    uint8_t ACB[ROW_ARITY] = {0, 1, 4, 5, 2, 3};
    auto key_length = 6;
    auto list_length = 2;

    auto plan = AssertSorted(
            new SegmentedSortOVC(
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


TEST_F(SegmentedSortTest, BigTest4OVC) {
    unsigned long domain = 16;
    unsigned long num_rows = 1 << 10;

    uint8_t ABC[ROW_ARITY] = {0, 1, 2, 3, 4, 5};
    uint8_t ACB[ROW_ARITY] = {0, 3, 4, 5, 1, 2};
    auto key_length = 6;

    auto plan = AssertSorted(
            new SegmentedSortOVC(
                    new RowBuffer(
                            new SortPrefixOVC(
                                    new GeneratorWithDomains(num_rows, {domain, domain, domain, domain, domain, domain},
                                                             SEED),
                                    key_length),
                            num_rows),
                            ABC, 1, 2, 3),
                    CmpColumnListOVC(ACB, key_length));

    plan.run();
    ASSERT_TRUE(plan.isSorted());
    ASSERT_TRUE(plan.getCount() == num_rows);
}