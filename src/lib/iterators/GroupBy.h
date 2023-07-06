#pragma once

#include "UnaryIterator.h"

template<bool USE_OVC>
class GroupByBase : public UnaryIterator {
private:
    Row input_buf;   // holds the first row of the next group
    Row output_buf;  // holds the Row we return in next
    bool empty;
    int group_columns;

public:
    explicit GroupByBase(Iterator *input, int group_columns);

    void open() override;

    Row *next() override;

    // overwritten, because we own the rows returned by next and the base method calls free on the input
    void free() override {};
};

typedef GroupByBase<true> GroupBy;
typedef GroupByBase<true> GroupByNoOVC;