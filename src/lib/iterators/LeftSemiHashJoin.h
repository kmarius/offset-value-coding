#pragma once

#include <unordered_set>
#include "Iterator.h"
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

        unsigned long getCount() {
            return count;
        }

    private:
        RowEqualPrefixNoOVC cmp;
        unsigned join_columns;
        std::vector<std::vector<Row>> set;
        std::vector<std::string> left_partitions;
        std::vector<std::string> right_partitions;
        io::ExternalRunR *left_partition;
        io::ExternalRunR *right_partition;
        io::BufferManager bufferManager;
        unsigned long count;

        bool nextPart();
    };
}