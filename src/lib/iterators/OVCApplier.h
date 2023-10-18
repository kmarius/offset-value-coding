#pragma once

#include "Iterator.h"
#include "../Row.h"

namespace ovc::iterators {

    class OVCApplier : public UnaryIterator {
    public:
        explicit OVCApplier(Iterator *input, int prefix = ROW_ARITY)
                : UnaryIterator(input), prev(), prefix(prefix), has_prev(false) {
        };

        Row *next() override {
            Row *row = input->next();
            if (row == nullptr) {
                return nullptr;
            }
            if (has_prev) {
                row->setOVC(prev, prefix, &stats);
            } else {
                row->setOVCInitial(ROW_ARITY);
                has_prev = true;
            }
            prev = *row;
            return row;
        };

        void open() override {
            Iterator::open();
            input->open();
        };

        void free() override {
            Iterator::free();
            input->free();
        };

        void close() override {
            Iterator::close();
            input->close();
        };

    private:
        Row prev;
        int prefix;
        bool has_prev;
    };
}