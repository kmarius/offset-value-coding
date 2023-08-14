#include "InStreamGroupBy.h"
#include "lib/log.h"

namespace ovc::iterators {

    template<bool USE_OVC, typename Aggregate>
    InStreamGroupByBase<USE_OVC, Aggregate>::InStreamGroupByBase(Iterator *input, int group_columns,
                                                                 const Aggregate &agg) :
            UnaryIterator(input), acc_buf(), output_buf(),
            group_columns(group_columns), empty(true), agg(agg),
            stats(), count(0) {
        assert(input->outputIsSorted());
        assert(!USE_OVC || input->outputHasOVC());
        output_is_unique = true;
    }

    template<bool USE_OVC, typename Aggregate>
    void InStreamGroupByBase<USE_OVC, Aggregate>::open() {
        Iterator::open();
        input_->open();
        Row *row = input_->next();
        if (row) {
            acc_buf = *row;
            agg.init(acc_buf);
            input_->free();
            empty = false;
        }
    }

    template<bool USE_OVC, typename Aggregate>
    Row *InStreamGroupByBase<USE_OVC, Aggregate>::next() {
        Iterator::next();

        if (empty) {
            return nullptr;
        }

        for (Row *row; (row = input_->next()) != nullptr; input_->free()) {
            agg.init(*row);
            if constexpr (USE_OVC) {
                if (row->getOVC().getOffset() < group_columns) {
                    output_buf = acc_buf;
                    agg.finalize(output_buf);

                    acc_buf = *row;
                    agg.init(acc_buf);
                    input_->free();

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
                        input_->free();

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
}