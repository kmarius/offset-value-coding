#pragma once

#include "Iterator.h"
#include "UnaryIterator.h"

class Filter : public UnaryIterator {
public :
    typedef bool (Predicate)(const Row *row);

    Filter(Iterator *input, Predicate *predicate);

    Row *next() override;

    bool outputIsHashed() override {
        return input_->outputIsHashed();
    }

    bool outputIsSorted() override {
        return input_->outputIsSorted();
    }

    bool outputHasOVC() override {
        // TODO: OVCs can be repaired in next
        return false;
    }

    bool outputIsUnique() override {
        return input_->outputIsUnique();
    }

private :
    Predicate *const predicate_;
};