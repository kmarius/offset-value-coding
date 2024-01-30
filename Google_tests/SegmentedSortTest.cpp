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

TEST_F(SegmentedSortTest, BigTest) {
    unsigned long domain = 128;
    unsigned long num_rows = 1 << 15;

    uint8_t A[ROW_ARITY] = {0, 1};
    uint8_t B[ROW_ARITY] = {2, 3};
    uint8_t ACB[ROW_ARITY] = {0, 1, 4, 5, 2, 3};
    uint8_t CB[ROW_ARITY] = {4, 5, 2, 3};

    auto plan = AssertSorted(
            new SegmentedSort(
                    new SortPrefix(
                            new GeneratorWithDomains(num_rows, {domain, domain, domain}, 1337),
                            sizeof ACB / sizeof *ACB),
                    EqColumnList(A, sizeof A / sizeof *A),
                    EqColumnList(B, sizeof B / sizeof *B),
                    CmpColumnList(CB, sizeof CB / sizeof *CB)),
            CmpColumnList(ACB, sizeof ACB / sizeof *ACB));

    plan.run();
    assert(plan.isSorted());;
    assert(plan.getCount() == num_rows);
}