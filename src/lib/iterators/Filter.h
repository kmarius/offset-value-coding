#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    class Filter : public UnaryIterator {
    public :
        typedef bool (Predicate)(const Row *row);

        Filter(Iterator *input, Predicate *predicate);

        void open() override;

        Row *next() override;

        void free() override;

        void close() override;

    private :
        Predicate *const predicate_;
    };
}