#pragma once

#include <unordered_set>
#include "lib/Partitioner.h"
#include "lib/io/ExternalRunR.h"
#include "Iterator.h"

namespace ovc::iterators {

    class HashDistinct : public UnaryIterator {
    public :
        explicit HashDistinct(Iterator *input, int prefix = ROW_ARITY);

        void open() override;

        Row *next() override;

        unsigned duplicates;

    private:
        std::vector<std::string> partitions;
        BufferManager bufferManager;
        std::vector<Row> rows;
        unsigned long ind;
        unsigned long count;
        int prefix;

        std::vector<Row> process_partition(const std::string &path);
    };
}