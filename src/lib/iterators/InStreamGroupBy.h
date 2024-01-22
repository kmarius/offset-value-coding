#pragma once

#include "Iterator.h"
#include "lib/comparators.h"

namespace ovc::iterators {
    using namespace ovc::comparators;

    template<typename Equals, typename Aggregate>
    class InStreamGroupByBase : public UnaryIterator {
    public:
        explicit InStreamGroupByBase(Iterator *input, int group_columns, const Equals &eq, const Aggregate &agg)
                : UnaryIterator(input), acc_buf(), output_buf(),
                  group_columns(group_columns), empty(true), eq(eq), agg(agg), count(0) {
        }

        void open() override {
            Iterator::open();
            input->open();
            Row *row = input->next();
            if (row) {
                acc_buf = *row;
                agg.init(acc_buf);
                input->free();
                empty = false;
            }
        }

        Row *next() override {
            Iterator::next();

            if (empty) {
                return nullptr;
            }

            for (Row *row; (row = input->next()) != nullptr; input->free()) {
                agg.init(*row);
                if constexpr (eq.USES_OVC) {
                    if (row->getOVC().getOffset() < group_columns) {
                        output_buf = acc_buf;
                        agg.finalize(output_buf);

                        acc_buf = *row;
                        agg.init(acc_buf);
                        input->free();

                        count++;
                        return &output_buf;
                    }
                } else {
                    for (int i = 0; i < group_columns; i++) {
                        stats.column_comparisons++;
                        if (row->columns[i] != acc_buf.columns[i]) {
                            output_buf = acc_buf;
                            agg.finalize(output_buf);

                            acc_buf = *row;
                            agg.init(acc_buf);
                            input->free();

                            count++;
                            return &output_buf;
                        }
                    }
                }
                agg.merge(acc_buf, *row);
            };

            // no more input
            empty = true;

            output_buf = acc_buf;
            agg.finalize(output_buf);

            count++;
            return &output_buf;
        }

        void close() override {
            Iterator::close();
            input->close();
        }

        unsigned long getCount() const {
            return count;
        }

    private:
        Aggregate agg;
        Equals eq;
        Row acc_buf;   // holds the first row of the group
        Row output_buf;  // holds the Row we return in next
        bool empty;
        int group_columns;
        unsigned long count;
    };

    template<typename Aggregate>
    class InStreamGroupByOVC : public InStreamGroupByBase<EqPrefixOVC, Aggregate> {
    public:
        InStreamGroupByOVC(Iterator *input, int groupColumns, const Aggregate &agg = Aggregate())
                : InStreamGroupByBase<EqPrefixOVC, Aggregate>(input, groupColumns,
                                                              EqPrefixOVC(groupColumns, &this->stats), agg) {};
    };

    template<typename Aggregate>
    class InStreamGroupBy : public InStreamGroupByBase<EqPrefix, Aggregate> {
    public:
        InStreamGroupBy(Iterator *input, int groupColumns, const Aggregate &agg = Aggregate())
                : InStreamGroupByBase<EqPrefix, Aggregate>(input, groupColumns,
                                                           EqPrefix(groupColumns, &this->stats), agg) {};
    };
}