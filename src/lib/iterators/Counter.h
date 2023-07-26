//
// Created by marius on 7/24/23.
//

#pragma once

#include "Iterator.h"

namespace ovc::iterators {
    template<typename T>
    class Counter : public Iterator {
    public:
        explicit Counter(T &&iter) : iter(std::move(iter)) {}

        void open() override {
            Iterator::open();
            iter.open();
        }

        Row *next() override {
            Iterator::next();
            return iter.next();
        }

        void free() override {
            Iterator::free();
            iter.free();
        }

        void close() override {
            Iterator::close();
            iter.close();
        }

        unsigned long getCount() const {
            return count;
        }

    private:
        T iter;
        unsigned long count;

    };
}