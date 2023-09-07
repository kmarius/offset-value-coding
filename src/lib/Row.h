#pragma once

#include <ostream>
#include <cassert>
#include <vector>
#include <cstdint>

#include "defs.h"
#include "log.h"

#define ROW_ARITY 8
#define DOMAIN 100

#define BITMASK(bits) ((1ul << (bits)) - 1)

// bits used for the offset, 7 mean we can work with 128 columns
#define ROW_OFFSET_BITS 7

#define ROW_OFFSET_MASK (BITMASK(ROW_OFFSET_BITS) << ROW_VALUE_BITS)

#define ROW_VALUE_BITS (8 * sizeof(ovc_type_t) - ROW_OFFSET_BITS)

#define ROW_VALUE_MASK (BITMASK(ROW_VALUE_BITS))

#define MAKE_OVC(arity, offset, value) (((arity - offset) << ROW_VALUE_BITS) & ROW_OFFSET_MASK | (value & ROW_VALUE_MASK))

namespace ovc {

    typedef uint32_t ovc_type_t;

    struct iterator_stats {
        size_t comparisons;
        size_t comparisons_equal_key;
        size_t comparisons_of_actual_rows;
        size_t column_comparisons;
        size_t rows_written;
        size_t rows_read;

        iterator_stats() = default;

        void add(const struct iterator_stats &stats) {
            comparisons += stats.comparisons;
            comparisons_equal_key += stats.comparisons_equal_key;
            comparisons_of_actual_rows += stats.comparisons_of_actual_rows;
            column_comparisons += stats.column_comparisons;
            rows_written += stats.rows_written;
            rows_read += stats.rows_read;
        }
    };

// statistics: number of times hash was called
    extern unsigned long row_num_calls_to_hash;

// statistics: number of times == was called
    extern unsigned long row_num_calls_to_equal;

    struct OVC_s {
        ovc_type_t ovc;

        OVC_s(ovc_type_t ovc) : ovc(ovc) {};

        unsigned getOffset() const {
            return ROW_ARITY - ((ovc & ROW_OFFSET_MASK) >> ROW_VALUE_BITS);
        };

        int getValue() const {
            return ovc & ROW_VALUE_MASK;
        };
    };

    typedef struct Row {
        OVC key;
        unsigned long tid;
        unsigned long columns[ROW_ARITY];

        OVC_s getOVC() const {
            return key;
        }

        inline void setOVC(const Row &base, int prefix = ROW_ARITY, struct iterator_stats *stats = nullptr) {
            OVC ovc = calcOVC(base, prefix, stats);
            key = ovc;
        }

        inline void setOVCInitial(int prefix = ROW_ARITY, struct iterator_stats *stats = nullptr) {
            key = MAKE_OVC(prefix, 0, columns[0]);
        }

        long calcOVC(const Row &base, int prefix = ROW_ARITY, struct iterator_stats *stats = nullptr) const {
            int offset = 0;
            long cmp;
            for (; offset < prefix && (cmp = columns[offset] - base.columns[offset]) == 0; offset++) {
                if (stats) {
                    stats->column_comparisons++;
                }
            }
            if (offset >= prefix) {
                return 0;
            }
            if (cmp < 0) {
                log_error("calcOVC: base is larger than row");
                return -1;
            }
            return MAKE_OVC(ROW_ARITY, offset, columns[offset]);
        }

        unsigned long setHash(int hash_columns = ROW_ARITY);

        bool equals(const Row &row) const {
            OVC ovc;
            return cmp(row, ovc, 0) == 0;
        };

        inline long cmp_impl(const Row &row, OVC &ovc, unsigned int offset, unsigned cmp_columns,
                             struct iterator_stats *stats) const {
            long cmp;

            for (; offset < cmp_columns && (cmp = columns[offset] - row.columns[offset]) == 0; offset++) {
                if (stats) {
                    stats->column_comparisons++;
                }
            }

            if (offset >= cmp_columns) {
                // rows are equal
                ovc = 0;
                return 0;
            }

            if (stats) {
                stats->column_comparisons++;
            }

            if (cmp < 0) {
                // we are smaller
                ovc = MAKE_OVC(ROW_ARITY, offset, row.columns[offset]);
            } else {
                // we are larger
                ovc = MAKE_OVC(ROW_ARITY, offset, columns[offset]);
            }
            return cmp;
        }


        /**
         * cmp semantics, writes the OVC of the loser w.r.t the winner to the provided address
         * @param row
         * @param ovc
         * @return
         */
        inline long
        cmp(const Row &row, OVC &ovc, unsigned int offset = 0, struct iterator_stats *stats = nullptr) const {
            return cmp_impl(row, ovc, offset, ROW_ARITY, stats);
        }

        /**
         * Get a string representation (in a statically allocated buffer).
         * @return The string.
         */
        const char *c_str(char *buf = nullptr) const {
            static char sbuf[128];
            if (buf == nullptr) {
                buf = sbuf;
            }
            int pos = sprintf(buf, "[%d@%d:%lu: ", getOVC().getValue(), getOVC().getOffset(), tid);
            for (int i = 0; i < ROW_ARITY; i++) {
                pos += sprintf(buf + pos, "%lu", columns[i]);
                if (i < ROW_ARITY - 1) {
                    pos += sprintf(buf + pos, ", ");
                }
            }
            sprintf(buf + pos, "]");
            return buf;
        }

        friend std::ostream &operator<<(std::ostream &stream, const Row &row);
    } Row;

    struct RowCmp {
        iterator_stats *stats;
    public:
        explicit RowCmp(iterator_stats *stats = nullptr) : stats(stats) {}

        int operator()(const Row &lhs, const Row &rhs, OVC &ovc, unsigned offset) const {
            return lhs.cmp(rhs, ovc, offset, stats);
        }

        int operator()(const Row &lhs, const Row &rhs) const {
            OVC ovc;
            return lhs.cmp(rhs, ovc, 0, nullptr);
        }
    };

    struct RowCmpPrefix {
        const int prefix;
        struct iterator_stats *stats;
    public:
        RowCmpPrefix() = delete;

        explicit RowCmpPrefix(int prefix, iterator_stats *stats = nullptr) : prefix(prefix), stats(stats) {};

        int operator()(const Row &lhs, const Row &rhs, OVC &ovc, unsigned offset) const {
            return lhs.cmp_impl(rhs, ovc, offset, prefix, stats);
        }

        int operator()(const Row &lhs, const Row &rhs) const {
            OVC ovc;
            return lhs.cmp_impl(rhs, ovc, 0, prefix, stats);
        }
    };

    // Compares two rows both having offset value codes relative to the same base row.
    // Sets the offset value code of the loser (bigger row) relative to the winner (smaller row).
    // If the rows are equal, the offset value code of the right row is set to 0.
    struct RowCmpPrefixOVC {
        const int prefix;
        struct iterator_stats *stats;
    public:
        RowCmpPrefixOVC() = delete;

        explicit RowCmpPrefixOVC(int prefix, iterator_stats *stats = nullptr) : prefix(prefix), stats(stats) {};

        long operator()(Row &lhs, Row &rhs) const {
            long cmp = lhs.key - rhs.key;
            if (cmp == 0) {
                if (lhs.key == 0) {
                    // equality, no need to go on
                    return 0;
                }

                // if offset > prefix, rows are equal
                int offset = lhs.getOVC().getOffset() + 1;

                OVC ovc;
                cmp = lhs.cmp_impl(rhs, ovc, offset, prefix, stats);

                if (cmp <= 0) {
                    rhs.key = ovc;
                }
                if (cmp > 0) {
                    lhs.key = ovc;
                }
            }
            return cmp;
        }
    };

    struct RowEqualPrefix {
        const int prefix;
        struct iterator_stats *stats;
    public:
        RowEqualPrefix() = delete;

        explicit RowEqualPrefix(const int prefix, struct iterator_stats *stats = nullptr) : prefix(prefix),
                                                                                            stats(stats) {}

        int operator()(const Row &lhs, const Row &rhs) const {
            for (int i = 0; i < prefix; i++) {
                if (stats) {
                    stats->column_comparisons++;
                }
                if (lhs.columns[i] != rhs.columns[i]) {
                    return false;
                }
            }
            return true;
        }
    };

    struct RowEqual {
        struct iterator_stats *stats;
    public:
        RowEqual() = delete;

        explicit RowEqual(struct iterator_stats *stats = nullptr) : stats(stats) {}

        int operator()(const Row &lhs, const Row &rhs) const {
            for (int i = 0; i < ROW_ARITY; i++) {
                if (stats) {
                    stats->column_comparisons++;
                }
                if (lhs.columns[i] != rhs.columns[i]) {
                    return false;
                }
            }
            return true;
        }
    };
}

namespace std {
    template<>
    struct hash<ovc::Row> {
        std::size_t operator()(const ovc::Row &p) const noexcept {
            ovc::row_num_calls_to_hash++;
            return p.key;
        }
    };
}