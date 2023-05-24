#pragma once

#include "Iterator.h"

class VectorScan : public Iterator {
public:
    explicit VectorScan(std::vector<Row> rows);

    ~VectorScan() override;

    void open() override;

    Row *next() override;

    void free() override;

    void close() override;
private:
    std::vector<Row> rows;
    size_t index;
};
