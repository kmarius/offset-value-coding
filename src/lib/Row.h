#pragma once

#include <ostream>
#include <cassert>
#include <vector>
#include <cstdint>

#include "defs.h"

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

    struct ovc_stats {
        size_t comparisons;
        size_t comparisons_equal_key;
        size_t comparisons_of_actual_rows;
        size_t column_comparisons;

        ovc_stats() = default;
    };

// statistics: column comparisons done in the == operator
    extern unsigned long row_equality_column_comparisons;

// statistics: number of times hash was called
    extern unsigned long row_num_calls_to_hash;

// statistics: number of times == was called
    extern unsigned long row_num_calls_to_equal;

    extern unsigned long row_equal_prefix;

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

        unsigned long setHash(int hash_columns = ROW_ARITY);

        /**
         * Check if two rows are equal.
         * @param row
         * @return
         */
        bool equals(const Row &row) const;

        bool equal_prefix(const Row &row) const {
            row_num_calls_to_equal++;
            for (int i = 0; i < row_equal_prefix; i++) {
                row_equality_column_comparisons++;
                if (columns[i] != row.columns[i]) {
                    return false;
                }
            }
            return true;
        }

        bool operator==(const Row &other) const {
            row_num_calls_to_equal++;
            for (int i = 0; i < ROW_ARITY; i++) {
                row_equality_column_comparisons++;
                if (columns[i] != other.columns[i]) {
                    return false;
                }
            }
            return true;
        };

        /**
         * cmp-than semantics
         * @param row
         * @return
         */
        bool less(const Row &row) const {
            for (int i = 0; i < ROW_ARITY; i++) {
                if (columns[i] != row.columns[i]) {
                    return columns[i] < row.columns[i];
                }
            }
            return false;
        }

        static bool
        cmp(const Row &lhs, const Row &rhs, OVC &ovc, unsigned int offset = 0, struct ovc_stats *stats = nullptr) {
            return lhs.less(rhs, ovc, offset, stats);
        }

        /**
         * cmp-than semantics, writes the OVC of the loser w.r.t the winner to the provided address
         * @param row
         * @param ovc
         * @return
         */
        inline bool less(const Row &row, OVC &ovc, unsigned int offset = 0, struct ovc_stats *stats = nullptr) const {
            long cmp;

#ifndef NDEBUG
            for (int i = 0; i < offset && i < ROW_ARITY; i++) {
                assert(columns[i] == row.columns[i]);
            }
#endif

            for (; offset < ROW_ARITY && (cmp = columns[offset] - row.columns[offset]) == 0; offset++) {
                if (stats) {
                    stats->column_comparisons++;
                }
            }

            if (offset >= ROW_ARITY) {
                // rows are equal
                ovc = 0;
                return false;
            }

            if (stats) {
                stats->column_comparisons++;
            }

            if (cmp < 0) {
                // we are smaller
                ovc = MAKE_OVC(ROW_ARITY, offset, row.columns[offset]);
                return true;
            } else {
                // we are larger
                ovc = MAKE_OVC(ROW_ARITY, offset, columns[offset]);
                return false;
            }
        }

        inline bool less_prefix(const Row &row, OVC &ovc, unsigned int offset, unsigned cmp_columns,
                                struct ovc_stats *stats) const {
            long cmp;

#ifndef NDEBUG
            for (int i = 0; i < offset && i < cmp_columns; i++) {
                assert(columns[i] == row.columns[i]);
            }
#endif

            for (; offset < cmp_columns && (cmp = columns[offset] - row.columns[offset]) == 0; offset++) {
                if (stats) {
                    stats->column_comparisons++;
                }
            }

            if (offset >= cmp_columns) {
                // rows are equal
                ovc = 0;
                return false;
            }

            if (stats) {
                stats->column_comparisons++;
            }

            if (cmp < 0) {
                // we are smaller
                ovc = MAKE_OVC(ROW_ARITY, offset, row.columns[offset]);
                return true;
            } else {
                // we are larger
                ovc = MAKE_OVC(ROW_ARITY, offset, columns[offset]);
                return false;
            }
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
            int pos = sprintf(buf, "[%d@%d:%lu: ", getOVC().getValue(), ROW_ARITY - getOVC().getOffset(), tid);
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
    public:
        bool operator()(const Row &lhs, const Row &rhs, OVC &ovc, unsigned offset, struct ovc_stats *stats) const {
            return lhs.less(rhs, ovc, offset, stats);
        }

        bool operator()(const Row &lhs, const Row &rhs) const {
            OVC ovc;
            return lhs.less(rhs, ovc, 0, nullptr);
        }
    };

    struct RowCmpPrefix {
        const int prefix;
    public:
        RowCmpPrefix() = delete;

        explicit RowCmpPrefix(int prefix) : prefix(prefix) {};

        bool operator()(const Row &lhs, const Row &rhs, OVC &ovc, unsigned offset, struct ovc_stats *stats) const {
            return lhs.less_prefix(rhs, ovc, offset, prefix, stats);
        }

        bool operator()(const Row &lhs, const Row &rhs) const {
            OVC ovc;
            return lhs.less_prefix(rhs, ovc, 0, prefix, nullptr);
        }
    };
}

namespace std {
    template<>
    struct hash<ovc::Row> {
        std::size_t operator()(const ovc::Row &p) const noexcept {
            ovc::row_num_calls_to_hash++;
            return p.key >> 8 | ((p.key & 0xFF) << (sizeof p.key - 8));
            // return p.key >> 8;
        }
    };
}