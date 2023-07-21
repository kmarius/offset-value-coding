#pragma once

#include <unordered_set>
#include "lib/Partitioner.h"
#include "lib/io/ExternalRunR.h"
#include "Iterator.h"

namespace ovc::iterators {

    class HashDistinct : public UnaryIterator {
    public :
        explicit HashDistinct(Iterator *input);

        void open() override;

        Row *next() override;

        unsigned duplicates;

    private:
        std::vector<std::string> partitions;
        ExternalRunR *partition;
        BufferManager bufferManager;
        std::unordered_set<Row> set;

        Row *next_from_part();
    };
}