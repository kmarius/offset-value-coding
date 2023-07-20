#pragma once

#include <unordered_set>
#include "BinaryIterator.h"
#include "lib/io/BufferManager.h"
#include "lib/io/ExternalRunR.h"

namespace ovc::iterators {
    class LeftSemiHashJoin : public BinaryIterator {
    public:

        LeftSemiHashJoin(Iterator *left, Iterator *right, int joinColumns);

        void open() override;

        Row *next() override;

        void free() override;

        void close() override;

    private:
        unsigned join_columns;
        std::vector<std::string> left_partitions;
        std::vector<std::string> right_partitions;
        io::ExternalRunR *left_partition;
        io::ExternalRunR *right_partition;
        io::BufferManager bufferManager;
        std::unordered_set<Row, std::hash<Row>, RowEqualPrefix> set;
    };
}
