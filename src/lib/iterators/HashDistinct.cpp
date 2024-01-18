
#include "HashDistinct.h"
#include "lib/log.h"
#include "lib/comparators.h"

namespace ovc::iterators {

    HashDistinct::HashDistinct(Iterator *input, int prefix) : UnaryIterator(input), bufferManager(4),
                                                  duplicates(0), prefix(prefix), ind(0) {
    }

    void HashDistinct::open() {
        Iterator::open();
        input->open();

        Partitioner partitioner(1 << RUN_IDX_BITS);

        // fill and probe external hashmap
        for (Row *row; (row = input->next()); input->free()) {
            row->setHash(prefix);
            partitioner.put(row);
        }
        partitioner.finalize();

        partitions = partitioner.getPartitionPaths();
        if (!partitions.empty()) {
            rows = process_partition(partitions.back());
        }
        input->close();

        stats.rows_written = partitioner.getStats().rows_written;
    }

    Row *HashDistinct::next() {
        if (partitions.empty()) {
            return nullptr;
        }

        if (ind >= rows.size()) {
            rows = {};
            ind = 0;
        }

        while (rows.empty()) {
            partitions.pop_back();
            if (partitions.empty()) {
                return nullptr;
            }
            rows = process_partition(partitions.back());
        }

        count++;
        return &rows[ind++];
    }

    std::vector<Row> HashDistinct::process_partition(const std::string &path) {
        auto eq = comparators::EqPrefixNoOVC(prefix, &stats);
        ExternalRunR part(path, bufferManager, true);
        if (part.definitelyEmpty()) {
            return {};
        }

        Partitioner partitioner(1 << RUN_IDX_BITS);

        for (Row *row; (row = part.read());) {
            stats.rows_read++;
            partitioner.putDistinct(row, eq);
        }

        part.remove();

        auto res = partitioner.finalizeDistinct();

        std::vector<std::string> new_partitions = partitioner.getPartitionPaths();
        for (auto &p: partitions) {
            new_partitions.push_back(p);
        }
        partitions = new_partitions;

        return res;
    }
}