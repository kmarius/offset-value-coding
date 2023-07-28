#pragma once

#include "Row.h"

namespace ovc::aggregates {

    struct Null {
        Null() {}

        void init(Row &row) const {
        }

        void merge(Row &acc, const Row &row) const {
        }

        void finalize(Row &row) const {
        }
    };

    struct Sum {
        Sum(int groupColumns, int aggColumn) : group_columns(groupColumns), agg_column(aggColumn) {}

        Sum() = delete;

        void init(Row &row) const {
            row.columns[group_columns] = row.columns[agg_column];
            for (int i = group_columns + 1; i < ROW_ARITY; i++) {
                row.columns[i] = 0;
            }
        }

        void merge(Row &acc, const Row &row) const {
            acc.columns[group_columns] += row.columns[group_columns];
        }

        void finalize(Row &row) const {
            // nothing
        }

    private:
        const int group_columns;
        const int agg_column;
    };

    struct Avg {
        Avg(int groupColumns, int aggColumn) : group_columns(groupColumns), agg_column(aggColumn) {}

        Avg() = delete;

        void init(Row &row) const {
            row.columns[group_columns] = row.columns[agg_column];
            row.columns[group_columns + 1] = 1;
            for (int i = group_columns + 2; i < ROW_ARITY; i++) {
                row.columns[i] = 0;
            }
        }

        void merge(Row &acc, const Row &row) const {
            acc.columns[group_columns] += row.columns[group_columns];
            acc.columns[group_columns + 1] += row.columns[group_columns + 1];
        }

        void finalize(Row &row) const {
            row.columns[group_columns] = row.columns[group_columns] / row.columns[group_columns + 1];
            row.columns[group_columns + 1] = 0;
        }

    private:
        const int group_columns;
        const int agg_column;
    };

    struct Count {
        explicit Count(int groupColumns) : group_columns(groupColumns) {}

        Count() = delete;

        void init(Row &row) const {
            row.columns[group_columns] = 1;
            for (int i = group_columns + 1; i < ROW_ARITY; i++) {
                row.columns[i] = 0;
            }
        }

        void merge(Row &acc, const Row &row) const {
            acc.columns[group_columns] += row.columns[group_columns];
        }

        void finalize(Row &row) const {
            // nothing
        }

    private:
        const int group_columns;
    };
}