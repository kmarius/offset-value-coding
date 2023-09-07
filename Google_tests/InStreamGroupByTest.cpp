#include "lib/Row.h"
#include "lib/iterators/InStreamGroupBy.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/AssertEqual.h"
#include "lib/log.h"
#include "lib/iterators/ZeroSuffixGenerator.h"
#include "lib/iterators/Sort.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace iterators;

class InStreamGroupByTest : public ::testing::Test {
protected:
    void SetUp() override {
        log_set_quiet(false);
        log_set_level(LOG_ERROR);
    }

    void TearDown() override {
    }
};

TEST_F(InStreamGroupByTest, EmptyTest) {
    auto *plan = new AssertEqual(
            new InStreamGroupBy(
                    new SortBase<false, true, RowCmpPrefix>(new ZeroSuffixGenerator(0, 0, 0), RowCmpPrefix{4}),
                    4,
                    aggregates::Count(1)),
            new VectorScan({})
    );
    plan->run();
    ASSERT_TRUE(plan->isEqual());
    delete plan;
}

TEST_F(InStreamGroupByTest, OneColumnSimple) {
    auto *plan = new AssertEqual(
            new InStreamGroupBy(new SortBase<false, true, RowCmpPrefix>(
                    new VectorScan({
                                           {0, 0, {0, 1}},
                                           {0, 1, {0, 2}},
                                           {0, 2, {1, 3}},
                                           {0, 3, {1, 4}},
                                           {0, 4, {1, 5}},
                                           {0, 3, {2, 6}},
                                           {0, 4, {2, 7}},
                                   }), RowCmpPrefix(1)), 1, aggregates::Count(1)),
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

TEST_F(InStreamGroupByTest, TwoColumnsSimple) {
    auto *plan = new AssertEqual(
            new InStreamGroupBy(new SortBase<false, true, RowCmpPrefix>(
                    new VectorScan({
                                           {0, 0, {0, 1, 5}},
                                           {0, 1, {0, 1, 5}},
                                           {0, 2, {1, 2, 1}},
                                           {0, 3, {1, 3, 4}},
                                           {0, 4, {1, 3, 7}},
                                           {0, 3, {2, 7, 2}},
                                           {0, 4, {2, 7, 9}},
                                   }), RowCmpPrefix{2}), 2, aggregates::Count(2)),
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

TEST_F(InStreamGroupByTest, DoesntLoseRows) {
    unsigned num_rows = 100000;
    int group_columns = 2;

    auto *plan = new InStreamGroupBy(
            new SortBase<false, true, RowCmpPrefix>(
                    new ZeroSuffixGenerator(num_rows, 32, 5),
                    RowCmpPrefix(group_columns)),
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

TEST_F(InStreamGroupByTest, DoesntLoseRows2) {
    unsigned num_rows = 100000;
    int group_columns = 4;

    auto *plan = new InStreamGroupBy(
            new SortBase<false, true, RowCmpPrefix>(new ZeroSuffixGenerator(num_rows, 32, 3),
                                                           RowCmpPrefix(group_columns)),
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


TEST_F(InStreamGroupByTest, OneColumnSimpleNoOvc) {
    auto *plan = new AssertEqual(
            new InStreamGroupByNoOvc(new Sort(
                                             new VectorScan({
                                                                    {0, 0, {0, 1}},
                                                                    {0, 1, {0, 2}},
                                                                    {0, 2, {1, 3}},
                                                                    {0, 3, {1, 4}},
                                                                    {0, 4, {1, 5}},
                                                                    {0, 3, {2, 6}},
                                                                    {0, 4, {2, 7}},
                                                            })),
                                     1,
                                     aggregates::Count(1)),
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

TEST_F(InStreamGroupByTest, TwoColumnsSimpleNoOvc) {
    auto *plan = new AssertEqual(
            new InStreamGroupByNoOvc(new Sort(
                                             new VectorScan({
                                                                    {0, 0, {0, 1, 5}},
                                                                    {0, 1, {0, 1, 5}},
                                                                    {0, 2, {1, 2, 1}},
                                                                    {0, 3, {1, 3, 4}},
                                                                    {0, 4, {1, 3, 7}},
                                                                    {0, 3, {2, 7, 2}},
                                                                    {0, 4, {2, 7, 9}},
                                                            })),
                                     2,
                                     aggregates::Count(2)),
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

TEST_F(InStreamGroupByTest, DoesntLoseRowsNoOvc) {
    unsigned num_rows = 100000;
    int group_columns = 2;

    auto *plan = new InStreamGroupByNoOvc(new Sort(new ZeroSuffixGenerator(num_rows, 32, 5)),
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

TEST_F(InStreamGroupByTest, DoesntLoseRows2NoOvc) {
    unsigned num_rows = 100000;
    int group_columns = 4;

    auto *plan = new InStreamGroupByNoOvc(new Sort(new ZeroSuffixGenerator(num_rows, 32, 3)),
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