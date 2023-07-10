
#include "HashDedup.h"
#include "lib/log.h"

HashDedup::HashDedup(Iterator *input) : UnaryIterator(input), partition(nullptr), bufferManager(4), duplicates(0) {
}

void HashDedup::open() {
    UnaryIterator::open();

    Partitioner partitioner(1 << RUN_IDX_BITS);

    // fill and probe external hashmap
    for (Row *row; (row = UnaryIterator::next()) ; UnaryIterator::free()) {
        row->setHash();
        partitioner.put(row);
    }
    partitioner.finalize();
    partitions = partitioner.getPartitions();
    partition = new ExternalRunR(partitions.back(), bufferManager, true);
}

Row *HashDedup::next() {
    for (Row *row; (row = next_from_part()); ) {
        auto pair = set.insert(*row);
        if (pair.second) {
            // element was actually inserted
            // maye return a pointer to the element in the set?
            return row;
        }
        duplicates++;
    }
    return nullptr;
}

Row *HashDedup::next_from_part() {
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