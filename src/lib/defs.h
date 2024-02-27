#pragma once

#include <cstddef>

// Use O_DIRECT to add through operating system caches
#define USE_O_DIRECT

// Use synchronuous IO (TODO)
//#define USE_SYNC_IO

// Where should runs be stored on disk?
// Memory (doesn't support O_DIRECT)
//#define BASEDIR "/tmp"

// TODO: create BASEDIR if it doesn't exist?
// SSD:
#define BASEDIR "/home/marius"

// HDD:
//#define BASEDIR "/home/marius/Data"

// This currently controls how many bits we use to store the run index in the key in the priority queue
// which at the same time controls the fan-in of the merge
// TODO: we should decouple this from the queue getSize
#define RUN_IDX_BITS 8

#define PRIORITYQUEUE_CAPACITY (1 << RUN_IDX_BITS)

#define LOGPATH "/tmp/ovc.log"
#define NO_LOGGING

//#define COLLECT_STATS

#define likely(x) __builtin_expect(x, 1)
#define unlikely(x) __builtin_expect(x, 0)

typedef unsigned long Count;
typedef unsigned long OVC;
typedef unsigned long Index;

struct iterator_stats {
    size_t comparisons;
    size_t comparisons_equal_key;
    size_t comparisons_of_actual_rows;
    size_t column_comparisons;
    size_t columns_hashed;
    size_t rows_written;
    size_t rows_read;
    size_t runs_generated; /* for segmented sorter only */
    size_t segments_found; /* for segmented sorter only */

    iterator_stats() = default;

    void add(const struct iterator_stats &stats) {
        comparisons += stats.comparisons;
        comparisons_equal_key += stats.comparisons_equal_key;
        comparisons_of_actual_rows += stats.comparisons_of_actual_rows;
        column_comparisons += stats.column_comparisons;
        columns_hashed += stats.columns_hashed;
        rows_written += stats.rows_written;
        rows_read += stats.rows_read;
        runs_generated += stats.runs_generated;
        segments_found += stats.segments_found;
    }
};