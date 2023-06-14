#pragma once

#include "Iterator.h"

class Filter : public Iterator {
public :
    typedef bool (Predicate)(Row *row);

    Filter(Predicate *predicate, Iterator *input);

    ~Filter() override;

    void open() override;

    void close() override;

    Row *next() override;

    void free() override;

    bool outputIsSorted() override {
        return input_->outputIsSorted();
    };

    bool outputIsHashed() override {
        return input_->outputIsHashed();
    };

private :
    Predicate *const predicate_;
    Iterator *const input_;
};
