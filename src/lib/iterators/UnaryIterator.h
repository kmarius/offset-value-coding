#pragma once

#include "Iterator.h"

class UnaryIterator : public Iterator {
public:

    explicit UnaryIterator(Iterator *input) : input_(input) {}

    ~UnaryIterator() override {
        delete input_;
    }

    void open() override {
        Iterator::open();
        input_->open();
    }

    void close() override {
        Iterator::close();
        input_->close();
    }

    Row *next() override {
        Iterator::next();
        return input_->next();
    }

    void free() override {
        Iterator::free();
        input_->free();
    }

protected:
    Iterator *input_;
};