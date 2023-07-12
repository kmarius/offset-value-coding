#pragma once

#include "Iterator.h"

namespace ovc::iterators {

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

        template<class T>
        T *getInput() {
            return reinterpret_cast<T *>(input_);
        }

    protected:
        Iterator *input_;
    };
}