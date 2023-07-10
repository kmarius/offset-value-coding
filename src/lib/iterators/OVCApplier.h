#pragma once

#include "UnaryIterator.h"

class OVCApplier : public UnaryIterator {
public:
    explicit OVCApplier(Iterator *input);

    Row *next() override;

    bool outputIsSorted() override {
        return true;
    }

    bool outputHasOVC() override {
        return true;
    }

    bool outputIsUnique() override {
        return input_->outputIsUnique();
    }

private:
    Row prev_;
};