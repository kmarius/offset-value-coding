#pragma once

#include "UnaryIterator.h"

namespace ovc::iterators {

    class OVCApplier : public UnaryIterator {
    public:
        explicit OVCApplier(Iterator *input);

        Row *next() override;

    private:
        Row prev_;
    };
}