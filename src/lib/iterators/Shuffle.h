#pragma once

#include "Iterator.h"

#include <vector>

class Shuffle : public Iterator {
public:
    explicit Shuffle(Iterator *input);

    ~Shuffle() override;

    void open() override;

    Row *next() override;

    void free() override;

    void close() override;

private:
    Iterator *input;
    unsigned long index;
    std::vector<Row> rows;
};

