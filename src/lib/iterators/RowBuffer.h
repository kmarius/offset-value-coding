#pragma once

#include "Iterator.h"

namespace ovc::iterators {
    class RowBuffer : public UnaryIterator {
    public:
        RowBuffer(Iterator *input, size_t size) : UnaryIterator(input), size(size), buf() {
            buf.reserve(size);
        }

        void open() override {
            Iterator::open();
            input->open();
        }

        Row *next() override {
            Iterator::next();
            Row *row = input->next();
            if (!row) {
                return nullptr;
            }
            assert(buf.size() < size);
            buf.push_back(*row);
            return &buf.back();
        }

        void free() override {
            Iterator::free();
            buf.clear();
        }

        void close() override {
            Iterator::close();
            input->close();
            buf = {};
        }

    private:
        size_t size;
        std::vector<Row> buf;
    };
}