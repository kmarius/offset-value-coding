
#include "HashDedup.h"
#include "lib/log.h"

HashDedup::HashDedup(Iterator *input) : input_(input), partition(nullptr), bufferManager(4) {
}

HashDedup::~HashDedup() {
    assert(status == Closed);
    delete input_;
}

void HashDedup::open() {
    Iterator::open();
    input_->open();

    Partitioner partitioner(1 << RUN_IDX_BITS);

    // fill and probe external hashmap
    for (Row *row = input_->next(); row; row = input_->next()) {
        row->setHash();
        partitioner.put(row);
    }
    partitioner.finalize();
    partitions = partitioner.getPartitions();
    partition = new ExternalRunR(partitions.back(), bufferManager);
}

void HashDedup::close() {
    Iterator::close();
    input_->close();
}

Row *HashDedup::next() {
    Iterator::next();

    Row *row;
    while ((row = next_from_part())) {
        auto pair = set.insert(*row);
        if (pair.second) {
            // element was actually inserted
            // maye return a pointer to the element in the set?
            return row;
        }
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
        partition = new ExternalRunR(partitions.back(), bufferManager);
    }
    return row;
}