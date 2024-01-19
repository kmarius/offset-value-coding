#include "lib/Row.h"
#include "lib/iterators/HashGroupBy.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/AssertEqual.h"
#include "lib/log.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/RowGenerator.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class HashGroupByTest : public ::testing::Test {
protected:
    void SetUp() override {
        log_set_quiet(false);
        log_set_level(LOG_ERROR);
    }

    void TearDown() override {
    }
};

TEST_F(HashGroupByTest, EmptyTest) {
    auto *plan = new AssertEqual(
            new HashGroupBy(new SortOVC(new RowGenerator(0, 0, 0)), 4, aggregates::Count(4)),
            new VectorScan({})
    );
    plan->run();
    ASSERT_TRUE(plan->isEqual());
    delete plan;
}

TEST_F(HashGroupByTest, OneColumnSimple) {
    auto *plan = new AssertEqual( new SortOVC(
            new HashGroupBy(
                    new VectorScan({
                                           {0, 0, {0, 1}},
                                           {0, 1, {0, 2}},
                                           {0, 2, {1, 3}},
                                           {0, 3, {1, 4}},
                                           {0, 4, {1, 5}},
                                           {0, 3, {2, 6}},
                                           {0, 4, {2, 7}},
                                   }), 1, aggregates::Count(1))),
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

TEST_F(HashGroupByTest, TwoColumnsSimple) {
    auto *plan = new AssertEqual(new SortOVC(
            new HashGroupBy(
                    new VectorScan({
                                           {0, 0, {0, 1, 5}},
                                           {0, 1, {0, 1, 5}},
                                           {0, 2, {1, 2, 1}},
                                           {0, 3, {1, 3, 4}},
                                           {0, 4, {1, 3, 7}},
                                           {0, 3, {2, 7, 2}},
                                           {0, 4, {2, 7, 9}},
                                   }), 2, aggregates::Count(2))),
            new VectorScan({
                                   {0, 0, {0, 1, 2}},
                                   {0, 0, {1, 2, 1}},
                                   {0, 0, {1, 3, 2}},
                                   {0, 0, {2, 7, 2}},
                           })
    );
    plan->run(true);
    ASSERT_TRUE(plan->isEqual());
    delete plan;
}

TEST_F(HashGroupByTest, DoesntLoseRows) {
    unsigned num_rows = 100000;
    int group_columns = 2;

    auto *plan = new HashGroupBy(new RowGenerator(num_rows, 32), group_columns, aggregates::Count(group_columns));
    plan->open();
    unsigned count = 0;
    for (Row *row; (row = plan->next()); plan->free()) {
        count += row->columns[group_columns];
    }
    plan->close();
    ASSERT_EQ(count, num_rows);
    delete plan;
}

TEST_F(HashGroupByTest, DoesntLoseRows2) {
    unsigned num_rows = 100000;
    int group_columns = 4;

    auto *plan = new HashGroupBy(new RowGenerator(num_rows, 32), group_columns, aggregates::Count(group_columns));
    plan->open();
    unsigned count = 0;
    for (Row *row; (row = plan->next()); plan->free()) {
        count += row->columns[group_columns];
    }
    plan->close();
    ASSERT_EQ(count, num_rows);
    delete plan;
}