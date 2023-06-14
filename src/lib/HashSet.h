#pragma once

#include <cstring>
#include "Row.h"

struct hash_set_stats {
    size_t hash_comparisons;
    size_t row_comparisons;
};

extern struct hash_set_stats hs_stats;

static inline void hash_set_stats_reset() {
    memset(&hs_stats, 0, sizeof hs_stats);
}

class HashSet {
public:
    explicit HashSet(int num_buckets);

    ~HashSet();

    /**
     * Insert a row into the hash set. The Row's `key` field is used for its hash value.
     * @param row The row to be inserted into this hash set.
     * @return true, if the row was inserted, false if it was a duplicate
     */
    bool put(Row *row);

    /**
     * Returns the next row from the hash set in unspecified order.
     * @return The row.
     */
    Row *next();

private:
    struct bucket;
    int num_buckets;
    int next_bucket;
    struct bucket *buckets;
};