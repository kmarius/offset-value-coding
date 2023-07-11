#pragma once

#include "UnaryIterator.h"

class OVCApplier : public UnaryIterator {
public:
    explicit OVCApplier(Iterator *input);

    Row *next() override;

private:
    Row prev_;
};