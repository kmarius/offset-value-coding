#include "LeftSemiHashJoin.h"
#include "lib/Partitioner.h"

namespace ovc::iterators {
    void LeftSemiHashJoin::open() {
        Iterator::open();

        Partitioner partitioner(1 << RUN_IDX_BITS);

        left->open();
        for (Row *row; (row = left->next()); left->free()) {
            row->setHash(join_columns);
            partitioner.put(row);
        }
        left->close();
        partitioner.finalize();
        left_partitions = partitioner.getPartitions();

        partitioner = Partitioner(1 << RUN_IDX_BITS);

        right->open();
        for (Row *row; (row = left->next()); left->free()) {
            row->setHash(join_columns);
            partitioner.put(row);
        }
        right->close();
        partitioner.finalize();
        right_partitions = partitioner.getPartitions();

        assert(left_partitions.size() == right_partitions.size());
    }

    Row *LeftSemiHashJoin::next() {
        Iterator::next();

        if (left_partition == nullptr) {
            return nullptr;
        }

        for (;;) {
            Row *row_left;
            while ((row_left = left_partition->read()) == nullptr) {
                left_partition->remove();
                right_partition->remove();
                delete left_partition;
                delete right_partition;
                left_partitions.pop_back();
                right_partitions.pop_back();
                set.clear();
                if (left_partitions.empty()) {
                    left_partition = nullptr;
                    right_partition = nullptr;
                    return nullptr;
                }
                left_partition = new ExternalRunR(left_partitions.back(), bufferManager);
                left_partition = new ExternalRunR(right_partitions.back(), bufferManager);
            }

            // fill hashset with right partition
            if (right_partition) {
                for (Row *row; (row = right_partition->read());) {
                    set.insert(*row);
                }
                right_partition->remove();
                delete right_partition;
                right_partition = nullptr;
            }

            // probe hashset of rows from the right partition
        }
    }

    void LeftSemiHashJoin::free() {
        Iterator::free();
    }

    void LeftSemiHashJoin::close() {
        Iterator::close();
    }
}