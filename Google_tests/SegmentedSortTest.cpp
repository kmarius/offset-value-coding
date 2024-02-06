#include "lib/Row.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/SegmentedSort.h"
#include "lib/iterators/AssertEqual.h"
#include "lib/iterators/GeneratorWithDomains.h"
#include "lib/iterators/AssertCount.h"
#include "lib/comparators.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/AssertSorted.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class SegmentedSortTest : public ::testing::Test {
protected:

    const size_t SEED = 1337;

    void SetUp() override {
        log_set_level(LOG_ERROR);
        log_set_quiet(true);
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

TEST_F(SegmentedSortTest, BigTest) {
    unsigned long domain = 16;
    unsigned long num_rows = 1 << 12;

    uint8_t A[ROW_ARITY] = {0, 1};
    uint8_t B[ROW_ARITY] = {2, 3};
    uint8_t ACB[ROW_ARITY] = {0, 1, 4, 5, 2, 3};
    uint8_t CB[ROW_ARITY] = {4, 5, 2, 3};

    auto plan = AssertSorted(
            new SegmentedSort(
                    new SortPrefix(
                            new GeneratorWithDomains(num_rows, {domain, domain, domain, domain, domain, domain}, SEED),
                            sizeof ACB / sizeof *ACB),
                    EqColumnList(A, sizeof A / sizeof *A),
                    EqColumnList(B, sizeof B / sizeof *B),
                    CmpColumnList(CB, sizeof CB / sizeof *CB)),
            CmpColumnList(ACB, sizeof ACB / sizeof *ACB));

    plan.run();
    assert(plan.isSorted());;
    assert(plan.getCount() == num_rows);
}

TEST_F(SegmentedSortTest, BigTest2) {
    unsigned long domain = 16;
    unsigned long num_rows = 1 << 12;

    uint8_t A[ROW_ARITY] = {0};
    uint8_t B[ROW_ARITY] = {1};
    uint8_t ACB[ROW_ARITY] = {0, 2, 1};
    uint8_t CB[ROW_ARITY] = {2, 1};

    auto plan = AssertSorted(
            new SegmentedSort(
                    new SortPrefix(
                            new GeneratorWithDomains(num_rows, {domain, domain, domain}, SEED),
                            sizeof ACB / sizeof *ACB),
                    EqColumnList(A, sizeof A / sizeof *A),
                    EqColumnList(B, sizeof B / sizeof *B),
                    CmpColumnList(CB, sizeof CB / sizeof *CB)),
            CmpColumnList(ACB, sizeof ACB / sizeof *ACB));

    plan.run();
    assert(plan.isSorted());;
    assert(plan.getCount() == num_rows);
}

TEST_F(SegmentedSortTest, BigTest2OVC) {
    unsigned long domain = 8;
    unsigned long num_rows = 1 << 12;

    uint8_t ABC[ROW_ARITY] = {0, 1, 2};
    uint8_t ACB[ROW_ARITY] = {0, 2, 1};
    auto list_length = 1;
    auto key_length = 3;

    auto plan = AssertSorted(
            new SegmentedSort(
                    new SortPrefixOVC(
                            new GeneratorWithDomains(num_rows, {domain, domain, domain}, SEED),
                            key_length),
                    EqOffset(ABC, key_length, list_length),
                    EqOffset(ABC, key_length, list_length * 2),
                    CmpColumnListDerivingOVC(ACB, key_length, list_length * 2, list_length)),
            CmpColumnListOVC(ACB, key_length));

    plan.run();
    assert(plan.isSorted());
    assert(plan.getCount() == num_rows);
}


TEST_F(SegmentedSortTest, BigTest3OVC) {
    unsigned long domain = 16;
    unsigned long num_rows = 1 << 10;

    uint8_t ABC[ROW_ARITY] = {0, 1, 2, 3, 4, 5};
    uint8_t ACB[ROW_ARITY] = {0, 1, 4, 5, 2, 3};
    auto key_length = 6;
    auto list_length = 2;

    auto plan = AssertSorted(
            new SegmentedSort(
                    new SortPrefixOVC(
                            new GeneratorWithDomains(num_rows, {domain, domain, domain, domain, domain, domain}, SEED),
                            key_length),
                    EqOffset(ABC, key_length, list_length),
                    EqOffset(ABC, key_length, list_length * 2),
                    CmpColumnListDerivingOVC(ACB, key_length, list_length * 2, list_length)),
            CmpColumnListOVC(ACB, key_length));

    plan.run();
    assert(plan.isSorted());;
    assert(plan.getCount() == num_rows);
}