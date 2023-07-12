#include "GroupBy.h"
#include "lib/log.h"

namespace ovc::iterators {

    template<bool USE_OVC>
    GroupByBase<USE_OVC>::GroupByBase(Iterator *input, int group_columns) :
            UnaryIterator(input), input_buf(), output_buf(),
            group_columns(group_columns), empty(true) {
        assert(input->outputIsSorted());
        assert(!USE_OVC || input->outputHasOVC());
        output_is_unique = true;
    }

    template<bool USE_OVC>
    void GroupByBase<USE_OVC>::open() {
        UnaryIterator::open();
        Row *row = UnaryIterator::next();
        if (row) {
            input_buf = *row;
            empty = false;
        }
    }

    template<>
    Row *GroupByBase<true>::next() {
        if (empty) {
            return nullptr;
        }

        int count = 1;
        for (int i = 0; i < group_columns; i++) {
            output_buf.columns[i] = input_buf.columns[i];
        }

        for (Row *row; (row = UnaryIterator::next()) != nullptr; UnaryIterator::free()) {
            if (row->getOVC().getOffset() < group_columns) {
                input_buf = *row;
                UnaryIterator::free();
                output_buf.columns[group_columns] = count;
                return &output_buf;
            }
            count++;
        };

        // no more input
        empty = true;

        output_buf.columns[group_columns] = count;
        return &output_buf;
    }

    template<>
    Row *GroupByBase<false>::next() {
        if (empty) {
            return nullptr;
        }

        int count = 1;
        for (int i = 0; i < group_columns; i++) {
            output_buf.columns[i] = input_buf.columns[i];
        }

        for (Row *row; (row = UnaryIterator::next()) != nullptr; UnaryIterator::free()) {
            for (int i = 0; i < group_columns; i++) {
                if (row->columns[i] != input_buf.columns[i]) {
                    input_buf = *row;
                    UnaryIterator::free();
                    output_buf.columns[group_columns] = count;
                    return &output_buf;
                }
            }
            count++;
        };

        // no more input
        empty = true;

        output_buf.columns[group_columns] = count;
        return &output_buf;
    }

    template
    class GroupByBase<true>;

    template
    class GroupByBase<false>;
}