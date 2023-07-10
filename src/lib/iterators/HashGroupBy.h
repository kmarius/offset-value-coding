#pragma once

#include <unordered_map>
#include "UnaryIterator.h"
#include "lib/io/ExternalRunR.h"

class HashGroupBy : public UnaryIterator {
    int group_columns;
    std::vector<std::string> partitions;
    BufferManager bufferManager;
    std::vector<Row> rows;
    unsigned long ind;

    std::vector<Row> process_partition(const std::string &path);

public:
    HashGroupBy(Iterator *input, int group_columns);

    void open() override;

    Row *next() override;

    void free() override {};

    void close() override;
};