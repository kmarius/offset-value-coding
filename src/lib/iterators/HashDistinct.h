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

        struct iterator_stats getStats() {
            return stats;
        }

    private:
        std::vector<std::string> partitions;
        ExternalRunR *partition;
        BufferManager bufferManager;
        std::unordered_set<Row> set;
        struct iterator_stats stats;

        Row *next_from_part();
    };
}