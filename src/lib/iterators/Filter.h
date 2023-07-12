#pragma once

#include "Iterator.h"
#include "UnaryIterator.h"

namespace ovc::iterators {

    class Filter : public UnaryIterator {
    public :
        typedef bool (Predicate)(const Row *row);

        Filter(Iterator *input, Predicate *predicate);

        Row *next() override;

    private :
        Predicate *const predicate_;
    };
}