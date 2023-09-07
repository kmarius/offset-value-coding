#pragma once

#include "Iterator.h"

namespace ovc::iterators {

    template<bool USE_OVC = false, typename Compare = RowCmpPrefix>
    class LeftSemiJoinBase : public BinaryIterator {
    public:
        LeftSemiJoinBase(Iterator *left, Iterator *right, unsigned join_columns)
                : BinaryIterator(left, right), cmp(Compare(join_columns, &stats)), row_right(nullptr),
                  row_left(nullptr), stats(), max_ovc(0), join_columns(join_columns), first_row(true) {
        }

        void open() override {
            Iterator::open();
            left->open();
            right->open();
            row_left = left->next();
            row_right = right->next();
        }

        Row *next() override {
            Iterator::next();
            for (;;) {
                if (row_right == nullptr) {
                    return nullptr;
                }

                if (row_left == nullptr) {
                    right->free();
                    row_right = nullptr;
                    return nullptr;
                }

                // This looks weird, but if the comparison shows equality, the offset value code of the right row will be
                // set to zero (it is the loser, since we are essentially keeping it in the "merge", left row is the output)
                long c = -cmp(*row_left, *row_right);

                if (c < 0) {
                    // right row is smaller, left row now has its OVC relative to this row (and so will the next right row)
                    if constexpr (USE_OVC) {
                        if (row_right->key > max_ovc) {
                            max_ovc = row_right->key;
                        }
                    }
                    right->free();
                    row_right = right->next();
                } else if (c > 0) {
                    // left row is smaller, right row now has its OVC relative to this row (and so will the next left row)
                    if constexpr (USE_OVC) {
                        if (row_left->key > max_ovc) {
                            max_ovc = row_left->key;
                        }
                    }
                    left->free();
                    row_left = left->next();
                } else {
                    // equality, right row has OVC relative to this one.
                    // to make sure this one has an ovc w.r.t the previously output row, apply the "max-rule"
                    if constexpr (USE_OVC) {
                        if (first_row) {
                            // the very first row we output should have its OVC w.r.t. to the non-existant row
                            row_left->setOVCInitial(join_columns, &stats);
                            first_row = false;
                        }
                        if (max_ovc > row_left->key) {
                            row_left->key = max_ovc;
                        }
                        max_ovc = 0;
                    }
                    return row_left;
                }
            }
        }

        void free() override {
            Iterator::free();
            left->free();
            row_left = left->next();
        }

        void close() override {
            Iterator::close();
            right->close();
            left->close();
        }

        const iterator_stats &getStats() const {
            return stats;
        }

        void accumulateStats(iterator_stats &stats) override {
            BinaryIterator::accumulateStats(stats);
            stats.add(this->stats);
        }

    private:
        Compare cmp;
        OVC max_ovc;
        Row *row_right;
        Row *row_left;
        unsigned join_columns;
        struct iterator_stats stats;
        bool first_row;
    };

    class LeftSemiJoin : public LeftSemiJoinBase<true, RowCmpPrefixOVC> {
    public:
        LeftSemiJoin(Iterator *left, Iterator *right, unsigned join_columns)
                : LeftSemiJoinBase<true, RowCmpPrefixOVC>(left, right, join_columns) {}
    };

    class LeftSemiJoinNoOVC : public LeftSemiJoinBase<false, RowCmpPrefix> {
    public:
        LeftSemiJoinNoOVC(Iterator *left, Iterator *right, unsigned join_columns)
                : LeftSemiJoinBase<false, RowCmpPrefix>(left, right, join_columns) {}
    };
}