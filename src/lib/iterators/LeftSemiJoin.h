#pragma once

#include "Iterator.h"
#include "lib/comparators.h"

namespace ovc::iterators {

    template<typename Compare = CmpPrefix>
    class LeftSemiJoinBase : public BinaryIterator {
    public:
        LeftSemiJoinBase(Iterator *left, Iterator *right, unsigned join_columns)
                : BinaryIterator(left, right), cmp(Compare(join_columns, &stats)), row_right(nullptr),
                  row_left(nullptr), max_ovc(0), join_columns(join_columns), first_row(true), count(0) {
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
                    if constexpr (cmp.uses_ovc) {
                        if (row_right->key > max_ovc) {
                            max_ovc = row_right->key;
                        }
                    }
                    right->free();
                    row_right = right->next();
                } else if (c > 0) {
                    // left row is smaller, right row now has its OVC relative to this row (and so will the next left row)
                    if constexpr (cmp.uses_ovc) {
                        if (row_left->key > max_ovc) {
                            max_ovc = row_left->key;
                        }
                    }
                    left->free();
                    row_left = left->next();
                } else {
                    // equality, right row has OVC relative to this one.
                    // to make sure this one has an ovc w.r.t the previously output row, apply the "max-rule"
                    if constexpr (cmp.uses_ovc) {
                        if (first_row) {
                            // the very first row we output should have its OVC w.r.t. to the non-existent row
                            row_left->setOVCInitial(ROW_ARITY, &stats);
                            first_row = false;
                        } else if (max_ovc > row_left->key) {
                            row_left->key = max_ovc;
                        }
                        max_ovc = 0;
                    }
                    count++;

                    //char buf[128];
                    //log_info("%s + %s", row_left->c_str(), row_right->c_str(buf));

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

        long getCount() const {
            return count;
        }

    private:
        Compare cmp;
        OVC max_ovc;
        Row *row_right;
        Row *row_left;
        unsigned join_columns;
        bool first_row;
        long count;
    };

    class LeftSemiJoinOVC : public LeftSemiJoinBase<CmpPrefixOVC> {
    public:
        LeftSemiJoinOVC(Iterator *left, Iterator *right, unsigned join_columns)
                : LeftSemiJoinBase<CmpPrefixOVC>(left, right, join_columns) {}
    };

    class LeftSemiJoin : public LeftSemiJoinBase<CmpPrefix> {
    public:
        LeftSemiJoin(Iterator *left, Iterator *right, unsigned join_columns)
                : LeftSemiJoinBase<CmpPrefix>(left, right, join_columns) {}
    };
}