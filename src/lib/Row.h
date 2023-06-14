#pragma once

#include <ostream>
#include <cassert>
#include <vector>

#include "defs.h"

#define ROW_ARITY 8
#define DOMAIN 100

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
    bool equals(const Row &row);

    /**
     * less-than semantics
     * @param row
     * @return
     */
    bool less(const Row &row) const;

    /**
     * less-than semantics, writes the OVC of the loser w.r.t the winner to the provided address
     * @param row
     * @param ovc
     * @return
     */
    bool less(const Row &row, OVC &ovc);

    /**
     * Get a string representation (in a statically allocated buffer).
     * @return The string.
     */
    const char *c_str() const;

    friend std::ostream &operator<<(std::ostream &stream, const Row &row);
} Row;