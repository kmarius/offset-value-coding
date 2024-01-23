#pragma once

#include "Row.h"

namespace ovc::aggregates {

    struct Null {
        static const bool IS_NULL = true;

        Null() = default;

        inline void init(Row &row) const {
        }

        inline void merge(Row &acc, const Row &row) const {
        }

        inline void finalize(Row &row) const {
        }
    };

    struct Sum {
        static const bool IS_NULL = false;

        Sum(int groupColumns, int aggColumn) : group_columns(groupColumns), agg_column(aggColumn) {}

        Sum() = delete;

        inline void init(Row &row) const {
            row.columns[group_columns] = row.columns[agg_column];
            for (int i = group_columns + 1; i < ROW_ARITY; i++) {
                row.columns[i] = 0;
            }
        }

        inline void merge(Row &acc, const Row &row) const {
            acc.columns[group_columns] += row.columns[group_columns];
        }

        inline void finalize(Row &row) const {
            // nothing
        }

    private:
        const int group_columns;
        const int agg_column;
    };

    struct Avg {
        static const bool IS_NULL = false;

        Avg(int groupColumns, int aggColumn) : group_columns(groupColumns), agg_column(aggColumn) {}

        Avg() = delete;

        inline void init(Row &row) const {
            row.columns[group_columns] = row.columns[agg_column];
            row.columns[group_columns + 1] = 1;
            for (int i = group_columns + 2; i < ROW_ARITY; i++) {
                row.columns[i] = 0;
            }
        }

        inline void merge(Row &acc, const Row &row) const {
            acc.columns[group_columns] += row.columns[group_columns];
            acc.columns[group_columns + 1] += row.columns[group_columns + 1];
        }

        inline void finalize(Row &row) const {
            row.columns[group_columns] = row.columns[group_columns] / row.columns[group_columns + 1];
            row.columns[group_columns + 1] = 0;
        }

    private:
        const int group_columns;
        const int agg_column;
    };

    struct Count {
        static const bool IS_NULL = false;

        explicit Count(int groupColumns) : group_columns(groupColumns) {}

        Count() = delete;

        inline void init(Row &row) const {
            row.columns[group_columns] = 1;
            for (int i = group_columns + 1; i < ROW_ARITY; i++) {
                row.columns[i] = 0;
            }
        }

        inline void merge(Row &acc, const Row &row) const {
            acc.columns[group_columns] += row.columns[group_columns];
        }

        inline void finalize(Row &row) const {
            // nothing
        }

    private:
        const int group_columns;
    };


    struct Max {
        static const bool IS_NULL = false;

        Max(int groupColumns, int aggColumn) : group_columns(groupColumns), agg_column(aggColumn) {}

        Max() = delete;

        inline void init(Row &row) const {
            row.columns[group_columns] = row.columns[agg_column];
            for (int i = group_columns + 1; i < ROW_ARITY; i++) {
                row.columns[i] = 0;
            }
        }

        inline void merge(Row &acc, const Row &row) const {
            if (acc.columns[group_columns] < row.columns[group_columns]) {
                acc.columns[group_columns] = row.columns[group_columns];
            }
        }

        inline void finalize(Row &row) const {
            // nothing
        }

    private:
        const int group_columns;
        const int agg_column;
    };
}