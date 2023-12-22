#include "LeftSemiHashJoin.h"
#include "lib/Partitioner.h"

namespace ovc::iterators {

    LeftSemiHashJoin::LeftSemiHashJoin(Iterator *left, Iterator *right, int joinColumns)
            : BinaryIterator(left, right), join_columns(joinColumns), left_partition(nullptr), right_partition(nullptr),
              bufferManager(8), cmp(joinColumns, &stats), count(0) {
        set.reserve(256);
        for (int i = 0; i < 256; i++) {
            set.emplace_back();
        }
    }

    void LeftSemiHashJoin::open() {
        Iterator::open();

        {
            Partitioner partitioner(1 << RUN_IDX_BITS);

            left->open();
            for (Row *row; (row = left->next()); left->free()) {
                row->setHash(join_columns);
                stats.columns_hashed += join_columns;
                partitioner.put(row);
            }
            left->close();
            partitioner.finalize(true);
            left_partitions = partitioner.getPartitionPaths();
        }

        {
            Partitioner partitioner = Partitioner(1 << RUN_IDX_BITS);

            right->open();
            for (Row *row; (row = right->next()); right->free()) {
                row->setHash(join_columns);
                stats.columns_hashed += join_columns;
                partitioner.put(row);
            }
            right->close();
            partitioner.finalize(true);
            right_partitions = partitioner.getPartitionPaths();
        };

        assert(left_partitions.size() == right_partitions.size());
        assert(left_partitions.size() > 0);

        assert(left_partitions.size() > 0);

        left_partition = new ExternalRunR(left_partitions.back(), bufferManager, true);
        right_partition = new ExternalRunR(right_partitions.back(), bufferManager, true);
    }

    Row *LeftSemiHashJoin::next() {
        Iterator::next();

        if (left_partition == nullptr) {
            return nullptr;
        }

        for (;;) {
            Row *row_left;
            // find a non-empty partition of the left input
            while (!(row_left = left_partition->read())) {
                left_partition->remove();
                if (right_partition) {
                    right_partition->remove();
                }
                if (!nextPart()) {
                    return nullptr;
                }
            }

            // fill hashset with right partition
            if (right_partition) {
                if (right_partition->definitelyEmpty()) {
                    if (!nextPart()) {
                        return nullptr;
                    }
                    continue;
                }
                for (Row *row; (row = right_partition->read());) {
                    uint64_t hash = row->key % 256;
                    set[hash].push_back(*row);
                }
                right_partition->remove();
                delete right_partition;
                right_partition = nullptr;
            }

            // probe hashset of rows from the right partition
            uint64_t hash = row_left->key % 256;
            for (auto &row : set[hash]) {
                if (row_left->key == row.key && cmp(*row_left, row)) {
                    count++;
                    return row_left;
                }
            }
        }
    }

    void LeftSemiHashJoin::free() {
        Iterator::free();
    }

    void LeftSemiHashJoin::close() {
        Iterator::close();
    }

    bool LeftSemiHashJoin::nextPart() {
        delete left_partition;
        delete right_partition;
        left_partitions.pop_back();
        right_partitions.pop_back();
        for (auto &v : set) {
            v.clear();
        }
        if (left_partitions.empty()) {
            left_partition = nullptr;
            right_partition = nullptr;
            return false;
        }
        left_partition = new ExternalRunR(left_partitions.back(), bufferManager, true);
        right_partition = new ExternalRunR(right_partitions.back(), bufferManager, true);
        return true;
    }
}