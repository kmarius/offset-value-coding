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
#include <sstream>

#define QUEUE_CAPACITY PRIORITYQUEUE_CAPACITY
#define MERGE_RUN_IDX ((1ul << RUN_IDX_BITS) - 2)
#define INITIAL_RUN_IDX 1

namespace ovc {

    typedef unsigned long Key;

    template<bool USE_OVC, typename Compare>
    class PriorityQueueBase {
    public:

        PriorityQueueBase(size_t capacity, iterator_stats *stats, const Compare &cmp = Compare());

        ~PriorityQueueBase();

        /**
         * Currently (possibly) doesn't check correctness with many fences also doesn't check ovc, only compares rows
         * @return
         */
        bool isCorrect() const;

        /**
         * The number of items in the queue.
         * @return The number of items in the queue.
         */
        inline size_t size() const {
            return size_;
        }

        /**
         * Check if the queue is empty.
         * @return true, if the queue is empty.
         */
        inline bool isEmpty() const {
            return size() == 0;
        }

        /**
         * Get the capacity of the queue.
         * @return The capacity of the queue.
         */
        size_t capacity() const {
            return capacity_;
        }

        /**
         * Reset the nodes in the heap, so that run_idx can start at 1 again. The queue must be empty.
         */
        inline void reset();

        void pass(Index index, Key key);


        /**
         * Push a row into the queue.
         * @param row The row.
         * @param run_index The run index of the row.
         */
        void push(Row *row, Index run_index, void *udata);

        /**
         * Pop the lowest row. The top element will be replaced with a low sentinel.
         * @param run_index The run index.
         * @return The row.
         */
        Row *pop(Index run_index);

        Row *top();

        void *top_udata();

        OVC top_ovc();

        size_t top_run_idx();

        std::string to_string() const;

        void flush_sentinels();

        void flush_sentinel(bool safe = false);

        Row *pop_safe(Index run_index);

        friend std::ostream &operator<<(std::ostream &o, const PriorityQueueBase<USE_OVC, Compare> &pq) {
            for (size_t i = 0; i < pq.capacity(); i++) {
                if (i > 0) {
                    o << std::endl;
                }
                o << "(slot=" << i << " " << pq.heap[i];
                if (pq.heap[i].isValid()) {
                    o << " " << *pq.workspace[pq.heap[i].index].row << ")";
                }
            }
            return o;
        }

    protected:
        iterator_stats *stats;

    private:
        struct WorkspaceItem;
        struct Node;

        size_t size_;
        size_t capacity_;
        Node *heap;
        WorkspaceItem *workspace;
        Compare cmp;
    };

    template<bool USE_OVC, typename Compare = comparators::CmpOVC>
    class PriorityQueue : public PriorityQueueBase<USE_OVC, Compare> {

    public:
        PriorityQueue(size_t capacity, iterator_stats *stats, const Compare &less = Compare())
                : PriorityQueueBase<USE_OVC, Compare>(capacity, stats, less) {
        };

        template<class T = void>
        inline void push(Row *row, Index run_index, T *udata = nullptr) {
            PriorityQueueBase<USE_OVC, Compare>::push(row, run_index, reinterpret_cast<void *>(udata));
        }

        template<class T = void>
        T *top_udata2() {
            return reinterpret_cast<T *>(this->top_udata());
        }

        inline void push_memory(MemoryRun &run) {
            push(run.front(), MERGE_RUN_IDX, &run);
        }

        inline void push_external(io::ExternalRunR &run) {
            Row *row = run.read();
            if (row) {
                push(row, MERGE_RUN_IDX, &run);
            }
        }

        /**
         * Pop the lowest row during run generation and replace it with a new one.
         * @param row The new row.
         * @param run_index The run index of the new row.
         * @param size 0 if inserting a single row, the offset of the in-memory run otherwise
         * @return The row at the head of the queue.
         */
        Row *pop_push(Row *row, Index run_index) {
            Row *res = this->pop(run_index);
            push(row, run_index);
            return res;
        }

        /**
         * Pop the lowest row during run generation and replace it with an in-memory run.
         * @param run The run.
         * @return The lowest row.
         */
        Row *pop_push_memory(MemoryRun *run) {
            Row *res = this->pop(MERGE_RUN_IDX);
            push(run->front(), MERGE_RUN_IDX, run);
            return res;
        }

        /**
         * Pop the lowest row in a merge step of in-memory memory_runs.
         * @return The row.
         */
        Row *pop_memory() {
            auto *run = this->template top_udata2<MemoryRun>();
            Row *res = this->pop(MERGE_RUN_IDX);
            run->next();

            if (likely(run->size() > 0)) {
                push(run->front(), MERGE_RUN_IDX, run);
            } else {
                this->flush_sentinel();
            }

            return res;
        }

        /**
         * Pop the lowest row in a merge step of external memory_runs.
         * @return
         */
        Row *pop_external() {
            auto *run = this->template top_udata2<io::ExternalRunR>();
            Row *res = this->pop(MERGE_RUN_IDX);
            Row *next = run->read();

            this->stats->rows_read++;

            if (likely(next != nullptr)) {
                push(next, MERGE_RUN_IDX, run);
            } else {
                this->flush_sentinel();
            }
            return res;
        }

        const std::string &top_path() {
            assert(!this->isEmpty());
            return top_udata2<io::ExternalRunR>()->path();
        };
    };
}

#include "PriorityQueue.ipp"
#include "comparators.h"
