#pragma once

#include "Iterator.h"

namespace ovc::iterators {
    using namespace ovc::comparators;

    template<typename Equals>
    class InStreamDistinctBase : public UnaryIterator {
    public:
        explicit InStreamDistinctBase(Iterator *input, const Equals &eq = Equals())
                : UnaryIterator(input), num_dupes(0), has_prev(false), prev({0}), eq(eq) {
        }

        void open() override {
            Iterator::open();
            input->open();
        }

        Row *next() override {
            for (Row *row; (row = input->next()); input->free()) {
                if constexpr (eq.USES_OVC) {
                    // TODO: we can repair OVCs here
                    if (row->key != 0) {
                        return row;
                    }
                } else {
                    if (!has_prev || !row->equals(prev)) {
                        prev = *row;
                        has_prev = true;
                        return row;
                    }
                }
                num_dupes++;
            }
            return nullptr;
        }

        void free() override {
            Iterator::free();
            input->free();
        }

        void close() override {
            Iterator::close();
            input->close();
        }

        size_t num_dupes;

    private:
        Equals eq;
        Row prev;
        bool has_prev;
    };

    class InStreamDistinctOVC : public InStreamDistinctBase<EqOVC> {
    public:
        explicit InStreamDistinctOVC(Iterator *input)
                : InStreamDistinctBase<EqOVC>(input, EqOVC(&this->stats)) {};
    };

    class InStreamDistinct : public InStreamDistinctBase<Eq> {
    public:
        explicit InStreamDistinct(Iterator *input)
                : InStreamDistinctBase<Eq>(input, Eq(&this->stats)) {};
    };
}