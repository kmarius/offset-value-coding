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

    template<typename Compare>
    class PriorityQueueBase {
    public:

        PriorityQueueBase(size_t max_capacity, iterator_stats *stats, const Compare &cmp = Compare());

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
        inline size_t getSize() const {
            return size;
        }

        /**
         * Check if the queue is empty.
         * @return true, if the queue is empty.
         */
        inline bool isEmpty() const {
            return getSize() == 0;
        }

        /**
         * Get the capacity of the queue.
         * @return The getCapacity of the queue.
         */
        inline size_t getCapacity() const {
            return capacity;
        }

        /**
        * Get the maximal capacity of the queue.
        * @return The getCapacity of the queue.
        */
        inline size_t getMaxCapacity() const {
            return max_capacity;
        }

        /**
         * Reset the nodes in the heap, so that run_idx can start at 1 again. The queue must be empty.
         */
        void reset();

        void reset(size_t capacity);

        void pass(Index index, Key key);

        /**
         * Push a row into the queue.
         * @param row The row.
         * @param run_index The run index of the row.
         */
        void push(Row *row, Index run_index, void *udata);

        void push_next(Row *row);

        /**
         * Pop the lowest row. The top element will be replaced with a low sentinel.
         * @return The row.
         */
        Row *pop();

        /* pop and flush */
        Row *popf();

        Row *top();

        void *top_udata();

        OVC top_ovc();

        size_t top_run_idx();

        std::string to_string() const;

        void flush_sentinels();

        void flush_sentinel(bool safe = false);

        Row *pop_safe(Index run_index);

        friend std::ostream &operator<<(std::ostream &o, const PriorityQueueBase<Compare> &pq) {
            for (size_t i = 0; i < pq.getCapacity(); i++) {
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

        Compare cmp;
    protected:
        iterator_stats *stats;

    private:
        struct WorkspaceItem;
        struct Node;

        size_t size; /* number of items in the queue */
        size_t capacity; /* current capacity, always <= max_capacity */
        size_t max_capacity; /* maximal capacity of the queue */
        Node *heap;
        WorkspaceItem *workspace;
    };

    template<typename Compare>
    class PriorityQueue : public PriorityQueueBase<Compare> {

    public:
        PriorityQueue(size_t capacity, iterator_stats *stats, const Compare &less = Compare())
                : PriorityQueueBase<Compare>(capacity, stats, less) {
        };

        template<class T = void>
        inline void push(Row *row, Index run_index, T *udata = nullptr) {
            PriorityQueueBase<Compare>::push(row, run_index, reinterpret_cast<void *>(udata));
        }

        template<class T = void>
        T *top_udata2() {
            return reinterpret_cast<T *>(this->top_udata());
        }

        inline void push_memory(MemoryRun &run) {
            push(run.front(), MERGE_RUN_IDX, &run);
        }

        inline void push_memory2(std::tuple<Row **, Row **> *run) {
            push(*std::get<0>(*run), MERGE_RUN_IDX, run);
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
            Row *res = this->pop();
            push(row, run_index);
            return res;
        }

        /**
         * Pop the lowest row during run generation and replace it with an in-memory run.
         * @param run The run.
         * @return The lowest row.
         */
        Row *pop_push_memory(MemoryRun *run) {
            Row *res = this->pop();
            push(run->front(), MERGE_RUN_IDX, run);
            return res;
        }

        /**
         * Pop the lowest row in a merge step of in-memory memory_runs.
         * @return The row.
         */
        Row *pop_memory() {
            auto *run = this->template top_udata2<MemoryRun>();
            Row *res = this->pop();
            run->next();

            if (likely(run->size() > 0)) {
                this->push_next(run->front());
            } else {
                this->flush_sentinel();
            }

            return res;
        }

        Row *pop_memory2() {
            auto run = this->template top_udata2<std::tuple<Row **,Row **>>();
            Row *res = this->pop();
            std::get<0>(*run)++;

            if (likely(std::get<0>(*run) < std::get<1>(*run))) {
                this->push_next(*std::get<0>(*run));
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
            Row *res = this->pop();
            Row *next = run->read();

#ifdef COLLECT_STATS
            this->stats->rows_read++;
#endif

            if (likely(next != nullptr)) {
                this->push_next(next);
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