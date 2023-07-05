#pragma once

#include "defs.h"
#include "log.h"
#include "lib/io/ExternalRunW.h"
#include "lib/io/ExternalRunR.h"
#include "Run.h"

#include <vector>
#include <cmath>
#include <bit>
#include <stack>

typedef unsigned long Key;

#define QUEUE_CAPACITY PRIORITYQUEUE_CAPACITY
#define MERGE_RUN_IDX ((1ul << RUN_IDX_BITS) - 2)
#define INITIAL_RUN_IDX 1

struct priority_queue_stats {
    size_t comparisons;
    size_t comparisons_equal_key;
    size_t comparisons_of_actual_rows;
};

extern struct priority_queue_stats stats;

void priority_queue_stats_reset();

template<bool USE_OVC>
class PriorityQueue {
private:
    struct Node;
    struct WorkspaceItem;

    size_t size_;
    size_t capacity_;
    Node *heap;
    WorkspaceItem *workspace;
    struct ovc_stats ovc_stats;

public:
    explicit PriorityQueue(size_t capacity);

    ~PriorityQueue();

    /**
     * Currently (possibly) doesn't check correctness with many fences also doesn't check ovc, only compares rows
     * @return
     */
    bool isCorrect() const;

    /**
     * The number of sort items in the queue.
     * @return The number of sort items in the queue.
     */
    size_t size() const;

    /**
     *
     * @return
     */
    bool isEmpty() const;

    /**
     * The capacity of the queue.
     * @return The capacity of the queue.
     */
    size_t capacity() const;

    /**
     * Push a row into the queue.
     * @param row The row.
     * @param run_index The run index of the row.
     */
    void push(Row *row, Index run_index);

    /**
     * Push an in-memory run into the queue.
     * @param row The first row of the run
     * @param size The offset of the run.
     */
    void push_memory(MemoryRun &run);

    /**
     * Push an external run into the queue.
     * @param run The external run.
     */
    void push_external(ExternalRunR &run);

    /**
     * Pop the lowest row during run generation.
     * @param run_index The run index of the high fence that will be inserted instead.
     * @return The row
     */
    Row *pop(Index run_index);

    Row *pop_safe(Index run_index);

    /**
     * Pop the lowest row during run generation and replace it with a new one.
     * @param row The new row.
     * @param run_index The run index of the new row.
     * @param size 0 if inserting a single row, the offset of the in-memory run otherwise
     * @return The row at the head of the queue.
     */
    Row *pop_push(Row *row, Index run_index);

    /**
     * Pop the lowest row during run generation and replace it with an in-memory run.
     * @param run The run.
     * @return The lowest row.
     */
    Row *pop_push_memory(MemoryRun *run);

    /**
     * Pop the lowest row in a merge step of in-memory memory_runs.
     * @return The row.
     */
    Row *pop_memory();

    /**
     * Pop the lowest row in a merge step of external memory_runs.
     * @return
     */
    Row *pop_external();

    friend std::ostream &operator<<(std::ostream &stream, const PriorityQueue<USE_OVC> &pq);

    std::string to_string() const;

    Row *top();

    size_t top_run_idx();

    const std::string &top_path();

    unsigned long getColumnComparisons() const {
        return ovc_stats.column_comparisons;
    }

    void flush_sentinels();

    void flush_sentinel(bool safe = false);

    /**
     * Reset the nodes in the heap, so that run_idx can start at 1 again.
     */
    void reset();

private:
    void pass(Index index, Key key);
};