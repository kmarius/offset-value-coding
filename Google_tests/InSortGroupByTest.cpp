#include "lib/Row.h"
#include "lib/iterators/InSortGroupBy.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/AssertEqual.h"
#include "lib/log.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/RowGenerator.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class InSortGroupByTest : public ::testing::Test {
protected:
    void SetUp() override {
        log_set_quiet(false);
        log_set_level(LOG_ERROR);
    }

    void TearDown() override {
    }
};

TEST_F(InSortGroupByTest, OVCEmptyTest) {
    auto *plan = new AssertEqual(
            new InSortGroupByOVC(new RowGenerator(0, 0, 0), 1, aggregates::Count(1)),
            new VectorScan({})
    );
    plan->run();
    ASSERT_TRUE(plan->isEqual());
    delete plan;
}

TEST_F(InSortGroupByTest, OVCOneColumnSimple) {
    auto *plan = new AssertEqual(
            new InSortGroupByOVC(
                    new VectorScan({
                                           {0, 0, {0, 1}},
                                           {0, 1, {0, 2}},
                                           {0, 2, {1, 3}},
                                           {0, 3, {1, 4}},
                                           {0, 4, {1, 5}},
                                           {0, 3, {2, 6}},
                                           {0, 4, {2, 7}},
                                   }), 1, aggregates::Count(1)),
            new VectorScan({
                                   {0, 0, {0, 2}},
                                   {0, 0, {1, 3}},
                                   {0, 0, {2, 2}},
                           })
    );
    plan->run();
    ASSERT_TRUE(plan->isEqual());
    delete plan;
}

TEST_F(InSortGroupByTest, OVCTwoColumnsSimple) {
    auto *plan = new AssertEqual(
            new InSortGroupByOVC(
                    new VectorScan({
                                           {0, 0, {0, 1, 5}},
                                           {0, 1, {0, 1, 5}},
                                           {0, 2, {1, 2, 1}},
                                           {0, 3, {1, 3, 4}},
                                           {0, 4, {1, 3, 7}},
                                           {0, 3, {2, 7, 2}},
                                           {0, 4, {2, 7, 9}},
                                   }), 2, aggregates::Count(2)),
            new VectorScan({
                                   {0, 0, {0, 1, 2}},
                                   {0, 0, {1, 2, 1}},
                                   {0, 0, {1, 3, 2}},
                                   {0, 0, {2, 7, 2}},
                           })
    );
    plan->run();
    ASSERT_TRUE(plan->isEqual());
    delete plan;
}

TEST_F(InSortGroupByTest, OVCDoesntLoseRows) {
    unsigned num_rows = 100000;
    int group_columns = 2;

    auto *plan = new InSortGroupByOVC(new RowGenerator(num_rows, 32, 5), group_columns,
                                      aggregates::Count(group_columns));
    plan->open();
    unsigned count = 0;
    for (Row *row; (row = plan->next()); plan->free()) {
        count += row->columns[group_columns];
    }
    plan->close();
    ASSERT_EQ(count, num_rows);
    delete plan;
}

TEST_F(InSortGroupByTest, OVCDoesntLoseRows2) {
    unsigned num_rows = 100000;
    int group_columns = 4;

    auto *plan = new InSortGroupByOVC(new RowGenerator(num_rows, 32, 3), group_columns,
                                      aggregates::Count(group_columns));
    plan->open();
    unsigned count = 0;
    for (Row *row; (row = plan->next()); plan->free()) {
        count += row->columns[group_columns];
    }
    plan->close();
    ASSERT_EQ(count, num_rows);
    delete plan;
}


TEST_F(InSortGroupByTest, OneColumnSimple) {
    auto *plan = new AssertEqual(
            new InSortGroupBy(
                    new VectorScan({
                                           {0, 0, {0, 1}},
                                           {0, 1, {0, 2}},
                                           {0, 2, {1, 3}},
                                           {0, 3, {1, 4}},
                                           {0, 4, {1, 5}},
                                           {0, 3, {2, 6}},
                                           {0, 4, {2, 7}},
                                   }), 1, aggregates::Count(1)),
            new VectorScan({
                                   {0, 0, {0, 2}},
                                   {0, 0, {1, 3}},
                                   {0, 0, {2, 2}},
                           })
    );
    plan->run();
    ASSERT_TRUE(plan->isEqual());
    delete plan;
}

TEST_F(InSortGroupByTest, TwoColumnsSimple) {
    auto *plan = new AssertEqual(
            new InSortGroupBy(
                    new VectorScan({
                                           {0, 0, {0, 1, 5}},
                                           {0, 1, {0, 1, 5}},
                                           {0, 2, {1, 2, 1}},
                                           {0, 3, {1, 3, 4}},
                                           {0, 4, {1, 3, 7}},
                                           {0, 3, {2, 7, 2}},
                                           {0, 4, {2, 7, 9}},
                                   }), 2, aggregates::Count(2)),
            new VectorScan({
                                   {0, 0, {0, 1, 2}},
                                   {0, 0, {1, 2, 1}},
                                   {0, 0, {1, 3, 2}},
                                   {0, 0, {2, 7, 2}},
                           })
    );
    plan->run();
    ASSERT_TRUE(plan->isEqual());
    delete plan;
}

TEST_F(InSortGroupByTest, DoesntLoseRows) {
    unsigned num_rows = 100000;
    int group_columns = 2;

    auto *plan = new InSortGroupBy(new RowGenerator(num_rows, 32, 5),
                                   group_columns,
                                   aggregates::Count(group_columns));
    plan->open();
    unsigned count = 0;
    for (Row *row; (row = plan->next()); plan->free()) {
        count += row->columns[group_columns];
    }
    plan->close();
    ASSERT_EQ(count, num_rows);
    delete plan;
}

TEST_F(InSortGroupByTest, DoesntLoseRows2) {
    unsigned num_rows = 100000;
    int group_columns = 4;

    auto *plan = new InSortGroupBy(new RowGenerator(num_rows, 32, 3),
                                   group_columns,
                                   aggregates::Count(group_columns));
    plan->open();
    unsigned count = 0;
    for (Row *row; (row = plan->next()); plan->free()) {
        count += row->columns[group_columns];
    }
    plan->close();
    ASSERT_EQ(count, num_rows);
    delete plan;
}