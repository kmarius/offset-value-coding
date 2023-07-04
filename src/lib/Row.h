#pragma once

#include <ostream>
#include <cassert>
#include <vector>
#include <cstdint>

#include "defs.h"

#define ROW_ARITY 8
#define DOMAIN 100

typedef uint32_t ovc_type_t;

#define BITMASK(bits) ((1ul << (bits)) - 1)

// bits used for the offset, 7 mean we can work with 128 columns
#define ROW_OFFSET_BITS 7

#define ROW_OFFSET_MASK (BITMASK(ROW_OFFSET_BITS) << ROW_VALUE_BITS)

#define ROW_VALUE_BITS (8 * sizeof(ovc_type_t) - ROW_OFFSET_BITS)

#define ROW_VALUE_MASK (BITMASK(ROW_VALUE_BITS))

#define MAKE_OVC(arity, offset, value) (((arity - offset) << ROW_OFFSET_BITS) & ROW_VALUE_MASK | (value & ROW_VALUE_MASK))

struct ovc_stats {
    unsigned long column_comparisons;

    explicit ovc_stats() : column_comparisons(0) {}
};

// statistics: column comparisons done in the == operator
extern unsigned long row_equality_column_comparisons;

// statistics: number of times hash was called
extern unsigned long row_num_calls_to_hash;

// statistics: number of times == was called
extern unsigned long row_num_calls_to_equal;

typedef struct Row {
    OVC key;
    unsigned long tid;
    unsigned long columns[ROW_ARITY];

    unsigned long setHash();

    /**
     * Check if two rows are equal.
     * @param row
     * @return
     */
    bool equals(const Row &row) const;

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
     * less-than semantics
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

    /**
     * less-than semantics, writes the OVC of the loser w.r.t the winner to the provided address
     * @param row
     * @param ovc
     * @return
     */
    inline bool less(const Row &row, OVC &ovc, unsigned int offset = 0, struct ovc_stats *stats = nullptr) {
        long cmp;
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

    /**
     * Get a string representation (in a statically allocated buffer).
     * @return The string.
     */
    const char *c_str() const;

    friend std::ostream &operator<<(std::ostream &stream, const Row &row);
} Row;

namespace std {
    template<>
    struct hash<Row> {
        std::size_t operator()(const Row &p) const noexcept {
            row_num_calls_to_hash++;
            return p.key >> 8 | ((p.key & 0xFF) << (sizeof p.key - 8));
            // return p.key >> 8;
        }
    };
}