#pragma once

#include <unordered_set>
#include "Iterator.h"
#include "lib/Partitioner.h"
#include "lib/io/ExternalRunR.h"

class HashDedup : public Iterator {
public :
    explicit HashDedup(Iterator *input);

    ~HashDedup() override;

    void open() override;

    void close() override;

    Row *next() override;


    bool outputIsHashed() override {
        return true;
    };

private :
    Iterator *input_;
    std::vector<std::string> partitions;
    ExternalRunR *partition;
    BufferManager bufferManager;
    std::unordered_set<Row> set;

    Row *next_from_part();
};
