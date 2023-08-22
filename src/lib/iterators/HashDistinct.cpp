
#include "HashDistinct.h"
#include "lib/log.h"

namespace ovc::iterators {

    HashDistinct::HashDistinct(Iterator *input) : UnaryIterator(input), partition(nullptr), bufferManager(4), duplicates(0), set(32, std::hash<Row>(), RowEqual(&stats)), stats() {
        output_is_unique = true;
        output_is_hashed = true;
    }

    void HashDistinct::open() {
        Iterator::open();
        input_->open();

        Partitioner partitioner(1 << RUN_IDX_BITS);

        // fill and probe external hashmap
        for (Row *row; (row = input_->next()); input_->free()) {
            row->setHash();
            partitioner.put(row);
        }
        partitioner.finalize();
        partitions = partitioner.getPartitions();
        partition = new ExternalRunR(partitions.back(), bufferManager, true);

        stats.rows_written = partitioner.getStats().rows_written;
    }

    Row *HashDistinct::next() {
        for (Row *row; (row = next_from_part());) {
            stats.rows_read++;
            auto pair = set.insert(*row);
            if (pair.second) {
                // element was actually i<1nserted
                // maye return a pointer to the element in the set?
                return row;
            }
            duplicates++;
        }
        return nullptr;
    }

    Row *HashDistinct::next_from_part() {
        if (partition == nullptr) {
            return nullptr;
        }
        Row *row;
        while ((row = partition->read()) == nullptr) {
            partition->remove();
            delete partition;
            partitions.pop_back();
            set.clear();
            if (partitions.empty()) {
                partition = nullptr;
                return nullptr;
            }
            partition = new ExternalRunR(partitions.back(), bufferManager, true);
        }
        return row;
    }
}