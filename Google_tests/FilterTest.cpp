
#include <gtest/gtest.h>
#include "lib/log.h"
#include "lib/iterators/Filter.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/AssertEqual.h"

using namespace ovc;
using namespace iterators;

class FilterTest : public ::testing::Test {
protected:

    const size_t SEED = 1337;

    void SetUp() override {
        log_set_quiet(true);
        log_set_level(LOG_ERROR);
    }

    void TearDown() override {

    }
};

TEST_F(FilterTest, EmptyTest) {
    auto *plan = new AssertEqual(
            new Filter(
                    new VectorScan(
                            {
                                    {0, 0, {0}},
                                    {0, 1, {1}},
                            }
                    ),
                    [](const Row *row) { return row->columns[0] % 2 == 0; }),
            new VectorScan(
                    {
                            {0, 0, {0}},
                    }
            ));
    plan->run();
    ASSERT_TRUE(plan->equal);
    delete plan;
}