#pragma once

#include "Iterator.h"
#include "../Row.h"

namespace ovc::iterators {

    class OVCApplier : public UnaryIterator {
    public:
        explicit OVCApplier(Iterator *input, int prefix = ROW_ARITY)
                : UnaryIterator(input), prev(), prefix(prefix), has_prev(false), stats() {
            output_is_sorted = true;
            output_has_ovc = true;
            output_is_unique = input->outputIsUnique();
        };

        Row *next() override {
            Row *row = input_->next();
            if (row == nullptr) {
                return nullptr;
            }
            if (has_prev) {
                row->setOVC(prev, prefix, &stats);
            } else {
                row->setOVCInitial(prefix);
                has_prev = true;
            }
            prev = *row;
            return row;
        };

        void open() override {
            Iterator::open();
            input_->open();
        };

        void free() override {
            Iterator::free();
            input_->free();
        };

        void close() override {
            Iterator::close();
            input_->close();
        };

        void accumulateStats(iterator_stats &stats) override {
            UnaryIterator::accumulateStats(stats);
            stats.add(this->stats);
        }

    private:
        Row prev;
        int prefix;
        bool has_prev;
        struct iterator_stats stats;
    };
}