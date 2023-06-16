#pragma once

#include "Iterator.h"
#include "UnaryIterator.h"

class Filter : public UnaryIterator {
public :
    typedef bool (Predicate)(const Row *row);

    Filter(Iterator *input, Predicate *predicate);

    Row *next() override;

    bool outputIsSorted() override;

    bool outputIsHashed() override;

    bool outputHasOVC() override;

private :
    Predicate *const predicate_;
};