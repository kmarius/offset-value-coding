#pragma once

#include "Iterator.h"

class VectorScan : public Iterator {
public:
    explicit VectorScan(std::vector<Row> rows);

    Row *next() override;

private:
    std::vector<Row> rows;
    size_t index;
};
