#pragma once

#include <unordered_map>
#include "Iterator.h"
#include "lib/io/ExternalRunR.h"

namespace ovc::iterators {

    class HashGroupBy : public UnaryIterator {
    public:
        HashGroupBy(Iterator *input, int group_columns);

        void open() override;

        Row *next() override;

        void close() override;

    private:
        int group_columns;
        std::vector<std::string> partitions;
        io::BufferManager bufferManager;
        std::vector<Row> rows;
        unsigned long ind;

        std::vector<Row> process_partition(const std::string &path);
    };
}