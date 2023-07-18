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

#define root() ((Index) 0)
#define parent(slot) ((slot) / 2)
#define left_child(index) (2 * index)
#define right_child(index) (2 * index + 1)

#define node_key_t uint64_t
#define node_index_t uint64_t
#define NODE_KEY_BITS (sizeof(node_key_t) * 8)
#define NODE_RUN_IDX_BITS (RUN_IDX_BITS)
#define NODE_OFFSET_BITS ROW_OFFSET_BITS

#define NODE_VALUE_BITS (ROW_VALUE_BITS)
#define NODE_VALUE_MASK ((node_key_t) (((node_key_t) 1) << NODE_VALUE_BITS) - 1)
#define NODE_VALUE(key) ((key) & NODE_VALUE_MASK)

#define NODE_OFFSET_MASK (((((node_key_t) 1) << NODE_OFFSET_BITS) - 1) << NODE_VALUE_BITS)
#define NODE_OFFSET(key) (ROW_ARITY - (((key) & NODE_OFFSET_MASK) >> NODE_VALUE_BITS))

#define NODE_OVC_MASK (NODE_OFFSET_MASK|NODE_VALUE_MASK)
#define NODE_OVC(key) ((key) & (NODE_OVC_MASK))

#define NODE_RUN_INDEX_MASK (((((node_key_t) 1) << NODE_RUN_IDX_BITS) - 1) << (NODE_OFFSET_BITS + NODE_VALUE_BITS))
#define NODE_RUN_INDEX(key) ((key & NODE_RUN_INDEX_MASK) >> (NODE_OFFSET_BITS + NODE_VALUE_BITS))

#define NODE_KEYGEN(run_index, key) ((((node_key_t) run_index) << (NODE_OFFSET_BITS + NODE_VALUE_BITS)) | key)

#define IS_LOW_SENTINEL(key) (NODE_RUN_INDEX(key) == 0)
#define IS_HIGH_SENTINEL(key) (((key) & NODE_RUN_INDEX_MASK) == NODE_RUN_INDEX_MASK)
#define LOW_SENTINEL(run_index) (NODE_KEYGEN(0, run_index))
#define HIGH_SENTINEL(run_index) (NODE_RUN_INDEX_MASK | (run_index))

namespace ovc {

    typedef unsigned long Key;

    template<bool USE_OVC>
    class PriorityQueueBase {
    public:

        PriorityQueueBase(size_t capacity) : capacity_(capacity), size_(0) {
            assert(std::__popcount(capacity) == 1);
            assert(PRIORITYQUEUE_CAPACITY >= (1 << RUN_IDX_BITS) - 3);
            workspace = new WorkspaceItem[capacity];
            heap = new Node[capacity];
            for (int i = 0; i < capacity_; i++) {
                heap[i].index = i;
                heap[i].key = LOW_SENTINEL(0);
            }
            log_info("PriorityQueue capacity=%lu", capacity);
        }

        ~PriorityQueueBase() {
            delete[] workspace;
            delete[] heap;
        }

        /**
         * Currently (possibly) doesn't check correctness with many fences also doesn't check ovc, only compares rows
         * @return
         */
        bool isCorrect() const {
            for (Index i = 0; i < capacity() / 2; i++) {
                for (Index j = capacity() / 2 + heap[i].index / 2; j > i; j = parent(j)) {
                    if (!heap[j].isValid()) {
                        continue;
                    }

                    if (heap[j].key <= heap[i].key &&
                        workspace[heap[j].index].row->less(*workspace[heap[i].index].row)) {
#ifndef NDEBUG
                        log_error("Element at position %lu is not smaller than the one in position %lu", i, j);
                    // TODO: log the queue here
#endif
                        return false;
                    }
                }
            }
            return true;
        }

        /**
         * The number of items in the queue.
         * @return The number of items in the queue.
         */
        inline size_t size() const {
            return size_;
        }

        /**
         *
         * @return
         */
        inline bool isEmpty() const {
            return size() == 0;
        }

        /**
         * The capacity of the queue.
         * @return The capacity of the queue.
         */
        size_t capacity() const {
            return capacity_;
        }

        /**
         * Push a row into the queue.
         * @param row The row.
         * @param run_index The run index of the row.
         */

        /**
         * Pop the lowest row during run generation.
         * @param run_index The run index of the high fence that will be inserted instead.
         * @return The row
         */

        /**
         * Reset the nodes in the heap, so that run_idx can start at 1 again. The queue must be empty.
         */
        inline void reset() {
            assert(isEmpty());
            for (int i = 0; i < capacity(); i++) {
                heap[i].key = LOW_SENTINEL(0);
            }
        }

        void pass(Index index, Key key) {
            Node candidate(index, key);
            for (Index slot = capacity_ / 2 + index / 2; slot != 0; slot /= 2) {
                if (heap[slot].less(candidate, workspace, &stats)) {
                    heap[slot].swap(candidate);
                }
            }
            heap[0] = candidate;
        }

        void push(Row *row, Index run_index, void *udata) {
            assert(size_ < capacity_);
            assert(heap[0].isLowSentinel());
            assert(row != nullptr);

            Index workspace_index = heap[0].index;

            workspace[workspace_index].row = row;
            workspace[workspace_index].udata = udata;
            pass(workspace_index, NODE_KEYGEN(run_index, row->key));
            size_++;
        }

        Row *pop(Index run_index) {
            assert(!isEmpty());
            assert(!heap[0].isLowSentinel());

            Index workspace_index = heap[0].index;

            Row *res = workspace[workspace_index].row;
            res->key = heap[0].ovc();

            // replace workspace item with high sentinel
            heap[0].key = LOW_SENTINEL(run_index);
            workspace[workspace_index].row = nullptr;
            size_--;

            return res;
        }

        Row *top() {
            assert(!isEmpty());
            return workspace[heap[0].index].row;
        }

        void *top_udata() {
            assert(!isEmpty());
            return workspace[heap[0].index].udata;
        }

        size_t top_run_idx() {
            return heap[0].run_index();
        }

        std::string to_string() const {
            std::stringstream stream;
            stream << *this << std::endl;
            return stream.str();
        }

        void flush_sentinels() {
            for (int i = size_; i < capacity_; i++) {
                Index workspace_index = heap[0].index;
                pass(workspace_index, HIGH_SENTINEL(MERGE_RUN_IDX));
            }
        }

        void flush_sentinel(bool safe = false) {
            if (safe) {
                if (heap[0].isLowSentinel()) {
                    pass(heap[0].index, HIGH_SENTINEL(MERGE_RUN_IDX));
                }
            } else {
                assert(heap[0].isLowSentinel());
                pass(heap[0].index, HIGH_SENTINEL(MERGE_RUN_IDX));
            }
        }

        Row *pop_safe(Index run_index) {
            while (heap[0].isLowSentinel()) {
                flush_sentinel();
            }
            return pop(run_index);
        }

        friend std::ostream &operator<<(std::ostream &o, const PriorityQueueBase<USE_OVC> &pq) {
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

        struct ovc_stats &getStats() {
            return stats;
        }

    private:
        struct WorkspaceItem {
            Row *row;
            void *udata;

            WorkspaceItem() = default;
        };

        struct Node {
            /* Sort key in this priority queue. */
            node_key_t key;

            /* Index of the corresponding element in the workspace (i.e. row, run2) */
            node_index_t index;

            Node() : key(0), index(0) {}

            Node(Index index, Key key) : key(key), index(index) {}

            inline void swap(Node &node) {
                Node tmp = node;
                node = *this;
                *this = tmp;
            }

            /* run_index of this element */
            inline Index run_index() const {
                return NODE_RUN_INDEX(key);
            }

            /* value (without the offset) */
            inline unsigned offset() const {
                return NODE_OFFSET(key);
            }

            /* value (without the offset) */
            inline OVC value() const {
                return NODE_VALUE(key);
            }

            /* offset value code */
            inline OVC ovc() const {
                return NODE_OVC(key);
            }

            /* true, if not a sentinel/fence value */
            inline bool isValid() const {
                return !IS_LOW_SENTINEL(key) && !IS_HIGH_SENTINEL(key);
            }

            inline bool isLowSentinel() const {
                return IS_LOW_SENTINEL(key);
            }

            inline bool isHighSentinel() const {
                return IS_HIGH_SENTINEL(key);
            }

            inline void setOvc(OVC ovc) {
                key &= ~NODE_OVC_MASK;
                key |= NODE_OVC_MASK & ovc;
                assert(this->ovc() == ovc);
            }

            const char *key_s() const {
                static char buf[128] = {0};
                sprintf(buf, "(off=%lu, val=%lu)", NODE_OFFSET(key), NODE_VALUE(key));
                return buf;
            }

            // sets ovc of the loser w.r.t. the winner
            inline bool less(Node &node, WorkspaceItem *ws, struct ovc_stats *stats = nullptr) {
                stats->comparisons++;

                if constexpr (!USE_OVC) {
                    stats->comparisons_equal_key++;
                    if (!isValid() || !node.isValid() || run_index() != node.run_index()) {
                        return key < node.key;
                    }
                    stats->comparisons_of_actual_rows++;
                    OVC ovc;
                    return ws[index].row->less(*ws[node.index].row, ovc, 0, stats);
                } else {
                    if (key == node.key) {
                        stats->comparisons_equal_key++;
                        if (!isValid() || !node.isValid()) {
                            return false;
                        }
                        stats->comparisons_of_actual_rows++;

                        OVC ovc;
                        if (ws[index].row->less(*ws[node.index].row, ovc, NODE_OFFSET(key) + 1, stats)) {
                            node.setOvc(ovc);
                            return true;
                        } else {
                            setOvc(ovc);
                            return false;
                        }
                    }

                    return key < node.key;
                }
            }

            friend std::ostream &operator<<(std::ostream &stream, const Node &node) {
                stream << "[key=" << node.key_s();
                if (node.isLowSentinel()) {
                    stream << ", run=" << node.value() << ", LO" << ":" << node.index << "]";
                } else if (node.isHighSentinel()) {
                    stream << ", run=" << node.value() << ", HI" << ":" << node.index << "]";
                } else {
                    stream << ", run=" << node.run_index() << ", val=" << node.value() << ": ind=" << node.index << "]";
                }
                return stream;
            }
        };

        size_t size_;
        size_t capacity_;
        Node *heap;
        WorkspaceItem *workspace;
        struct ovc_stats stats;
    };

    template<bool USE_OVC>
    class PriorityQueue : public PriorityQueueBase<USE_OVC> {

    public:
        explicit PriorityQueue(size_t capacity) : PriorityQueueBase<USE_OVC>(capacity) {};

        template<class T = void>
        inline void push(Row *row, Index run_index, T *udata = nullptr) {
            PriorityQueueBase<USE_OVC>::push(row, run_index, reinterpret_cast<void *>(udata));
        }

        template<class T = void>
        T *top_udata2() {
            return reinterpret_cast<T *>(this->top_udata());
        }

        inline void push_memory(MemoryRun &run) {
            push(run.front(), MERGE_RUN_IDX, &run);
        }

        inline void push_external(io::ExternalRunR &run) {
            push(run.read(), MERGE_RUN_IDX, &run);
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