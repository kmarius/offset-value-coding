#include "lib/Row.h"
#include "lib/io/ExternalRunR.h"
#include "lib/io/ExternalRunW.h"
#include "lib/io/ExternalRunRS.h"
#include "lib/io/ExternalRunWS.h"
#include "lib/log.h"

#include <gtest/gtest.h>

using namespace ovc;
using namespace io;

class ExternalRunTest : public ::testing::Test {
protected:
    std::string path_sync = "/tmp/ExternalRunTest-sync.dat";
    std::string path_async = "/tmp/ExternalRunTest-async.dat";
    std::string path_dummy = "/tmp/ExternalRunTest-dummy.dat";

    void SetUp() override {
        log_set_quiet(true);

        size_t num_rows = 1024;

        ExternalRunWS run_sync(path_sync);
        ExternalRunWS run_async(path_async);

        for (size_t i = 0; i < num_rows; i++) {
            Row row = {0, i};
            run0.push_back(row);
            run_sync.add(row);
            run_async.add(row);
        }
    }

    void TearDown() override {
        ::remove(path_sync.c_str());
        ::remove(path_async.c_str());
        ::remove(path_dummy.c_str());
    }

    BufferManager buffer_manager;
    std::vector<Row> run0;
};

TEST_F(ExternalRunTest, CanOpenReadSync) {
    ExternalRunRS run1(path_sync);
}

TEST_F(ExternalRunTest, CanOpenWriteSync) {
    ExternalRunWS run1(path_dummy);
}

TEST_F(ExternalRunTest, CanOpenReadAsync) {
    ExternalRunR run1(path_sync, buffer_manager);
}

TEST_F(ExternalRunTest, CanOpenWriteAsync) {
    ExternalRunW run1(path_dummy, buffer_manager);
}

TEST_F(ExternalRunTest, CanReadRunSyncCorrectly) {
    ExternalRunRS run1(path_sync);

    Row *row1;

    for (const auto & row0 : run0) {
        row1 = run1.read();
        EXPECT_NE(row1, nullptr);
        EXPECT_TRUE(row1->equals(row0));
    }

    row1 = run1.read();
    EXPECT_EQ(row1, nullptr);
}

TEST_F(ExternalRunTest, CanReadRunAsyncCorrectly) {
    ExternalRunR run1(path_async, buffer_manager);

    Row *row1;

    for (const auto & row0 : run0) {
        row1 = run1.read();
        EXPECT_NE(row1, nullptr);
        EXPECT_TRUE(row1->equals(row0));
    }

    row1 = run1.read();
    EXPECT_EQ(row1, nullptr);
}

TEST_F(ExternalRunTest, CanReadRunMixedCorrectly) {
    ExternalRunRS run_sync(path_async);
    ExternalRunR run_async(path_sync, buffer_manager);

    Row *row1, *row2;

    for (const auto & row0 : run0) {
        row1 = run_sync.read();
        row2 = run_async.read();
        EXPECT_NE(row1, nullptr);
        EXPECT_NE(row2, nullptr);
        EXPECT_TRUE(row1->equals(row0));
        EXPECT_TRUE(row2->equals(row0));
    }

    row1 = run_sync.read();
    EXPECT_EQ(row1, nullptr);
    row2 = run_async.read();
    EXPECT_EQ(row2, nullptr);
}