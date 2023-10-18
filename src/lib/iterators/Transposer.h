#pragma once

#include "Iterator.h"
#include "../Row.h"

namespace ovc::iterators {
    class Transposer : public UnaryIterator {

    public:
        Transposer(Iterator *input, int index) : UnaryIterator(input), index(index) {
            assert(index < ROW_ARITY/2);
        }

        virtual void open() {
            UnaryIterator::open();
            input->open();
        }

        virtual Row *next() {
            UnaryIterator::next();
            Row *row = input->next();
            if (!row) {
                return nullptr;
            }
            for (int i = 0; i < index; i++) {
                long tmp = row->columns[i];
                row->columns[i] = row->columns[i + index];
                row->columns[i + index] = tmp;
            }
            return row;
        }

        virtual void free() {
            UnaryIterator::free();
            input->free();
        }

        virtual void close() {
            UnaryIterator::close();
            input->close();
        }

    private:
        int index;
    };
}
