#pragma once

#include <unordered_map>
#include "Iterator.h"
#include "lib/io/ExternalRunR.h"

namespace ovc::iterators {

    template<typename Aggregate>
    class HashGroupBy : public UnaryIterator {
    public:
        HashGroupBy(Iterator *input, int group_columns, const Aggregate &agg = Aggregate());

        void open() override;

        Row *next() override;

        void close() override;

        struct iterator_stats &getStats() {
            return stats;
        }

        unsigned long getCount() const {
            return count;
        }

    private:
        Aggregate agg;
        int group_columns;
        std::vector<std::string> partitions;
        io::BufferManager bufferManager;
        std::vector<Row> rows;
        unsigned long ind;
        unsigned long count;
        struct iterator_stats stats;

        std::vector<Row> process_partition(const std::string &path);
    };
}

#include "HashGroupBy.ipp"