#pragma once

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
#define LOW_SENTINEL(index) (NODE_KEYGEN(0, index))
#define HIGH_SENTINEL(index) (NODE_RUN_INDEX_MASK | (index))

namespace ovc {

    template<typename Compare>
    struct PriorityQueueBase<Compare>::WorkspaceItem {
        Row *row;
        void *udata;

        WorkspaceItem() = default;
    };

    template<typename Compare>
    struct PriorityQueueBase<Compare>::Node {
        /* SortOVC key in this priority queue. */
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

        inline void setOVC(OVC ovc) {
            key &= ~NODE_OVC_MASK;
            key |= NODE_OVC_MASK & ovc;
            assert(this->ovc() == ovc);
        }

        const char *key_s() const {
            static char buf[128] = {0};
            sprintf(buf, "(off=%lu, offset=%lu)", NODE_OFFSET(key), NODE_VALUE(key));
            return buf;
        }

        // sets ovc of the loser w.r.t. the winner
        inline bool less(Node &node, Compare &cmp, WorkspaceItem *ws, struct iterator_stats *stats) {

#ifdef COLLECT_STATS
            stats->comparisons++;
#endif

            if constexpr (!cmp.USES_OVC) {
                if (key == node.key) {
                    // same run index, need to compare rows
#ifdef COLLECT_STATS
                    stats->comparisons_equal_key++;
                    stats->comparisons_of_actual_rows++;
#endif
                    return cmp(*ws[index].row, *ws[node.index].row) < 0;
                }

                return key < node.key;
            } else {
                if (key == node.key) {
#ifdef COLLECT_STATS
                    stats->comparisons_equal_key++;
                    stats->comparisons_of_actual_rows++;
#endif
                    if (cmp(*ws[index].row, *ws[node.index].row) <= 0) {
                        node.setOVC(ws[node.index].row->key);
                        return true;
                    } else {
                        setOVC(ws[index].row->key);
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
                stream << ", run=" << node.run_index() << ", offset=" << node.value() << ": ind=" << node.index << "]";
            }
            return stream;
        }
    };

    template<typename Compare>
    PriorityQueueBase<Compare>::PriorityQueueBase(size_t capacity, iterator_stats *stats, const Compare &cmp)
            : capacity_(capacity), size_(0), cmp(cmp), stats(stats) {
        assert(std::__popcount(capacity) == 1);
        assert(PRIORITYQUEUE_CAPACITY >= (1 << RUN_IDX_BITS) - 3);
        workspace = new WorkspaceItem[capacity];
        heap = new Node[capacity];
        for (int i = 0; i < capacity_; i++) {
            heap[i].index = i;
            heap[i].key = LOW_SENTINEL(i);
        }
    }

    template<typename Compare>
    bool PriorityQueueBase<Compare>::isCorrect() const {
        for (Index i = 0; i < capacity() / 2; i++) {
            for (Index j = capacity() / 2 + heap[i].index / 2; j > i; j = parent(j)) {
                if (!heap[j].isValid()) {
                    continue;
                }
                long c;
                if (heap[j].key <= heap[i].key &&
                    (c = cmp.raw(*workspace[heap[i].index].row, *workspace[heap[j].index].row)) > 0) {
#ifndef NDEBUG

                    log_error("Element at position %lu is not smaller than the one in position %lu, cmp=%lu", i, j, c
                    );
                    log_error("position %d: sortkey=%lu %s", i, heap[i].key, workspace[heap[i].index].row->c_str());
                    log_error("position %d: sortkey=%lu %s", j, heap[j].key, workspace[heap[j].index].row->c_str());

                    log_error("\n%s", to_string().c_str());
#endif
                    return false;
                }
            }
        }
        return true;
    }

    template<typename Compare>
    PriorityQueueBase<Compare>::~PriorityQueueBase() {
        delete[] workspace;
        delete[] heap;
    }

    template<typename Compare>
    Row *PriorityQueueBase<Compare>::pop_safe(Index run_index) {
        while (heap[0].isLowSentinel()) {
            flush_sentinel();
        }
        return pop(run_index);
    }

    template<typename Compare>
    void PriorityQueueBase<Compare>::flush_sentinel(bool safe) {
        if (safe) {
            if (heap[0].isLowSentinel()) {
                pass(heap[0].index, HIGH_SENTINEL(heap[0].index));
            }
        } else {
            assert(heap[0].isLowSentinel());
            pass(heap[0].index, HIGH_SENTINEL(heap[0].index));
        }
    }

    template<typename Compare>
    void PriorityQueueBase<Compare>::flush_sentinels() {
        for (int i = size_; i < capacity_; i++) {
            assert(heap[0].isLowSentinel());
            pass(heap[0].index, HIGH_SENTINEL(heap[0].index));
        }
    }

    template<typename Compare>
    std::string PriorityQueueBase<Compare>::to_string() const {
        std::stringstream stream;
        stream << *this << std::endl;
        return stream.str();
    }

    template<typename Compare>
    size_t PriorityQueueBase<Compare>::top_run_idx() {
        assert(!isEmpty());
        return heap[0].run_index();
    }

    template<typename Compare>
    void *PriorityQueueBase<Compare>::top_udata() {
        assert(!isEmpty());
        return workspace[heap[0].index].udata;
    }

    template<typename Compare>
    OVC PriorityQueueBase<Compare>::top_ovc() {
        return heap[0].ovc();
    }

    template<typename Compare>
    Row *PriorityQueueBase<Compare>::top() {
        assert(!isEmpty());
        assert(!heap[0].isLowSentinel());
        return workspace[heap[0].index].row;
    }

    template<typename Compare>
    Row *PriorityQueueBase<Compare>::pop(Index run_index) {
        assert(!isEmpty());
        assert(!heap[0].isLowSentinel());

        Index workspace_index = heap[0].index;

        Row *res = workspace[workspace_index].row;

        // replace node with low sentinel
        heap[0].key = LOW_SENTINEL(workspace_index);
        workspace[workspace_index].row = nullptr;
        size_--;

        return res;
    }

    template<typename Compare>
    void PriorityQueueBase<Compare>::push(Row *row, Index run_index, void *udata) {
        assert(size_ < capacity_);
        assert(heap[0].isLowSentinel());
        assert(row != nullptr);

        Index workspace_index = heap[0].index;

        workspace[workspace_index].row = row;
        workspace[workspace_index].udata = udata;
        if constexpr (cmp.USES_OVC) {
            pass(workspace_index, NODE_KEYGEN(run_index, row->key));
        } else {
            pass(workspace_index, NODE_KEYGEN(run_index, 0));
        }
        size_++;
    }

    template<typename Compare>
    void PriorityQueueBase<Compare>::pass(Index index, Key key) {
        Node candidate(index, key);
        for (Index slot = capacity_ / 2 + index / 2; slot != 0; slot /= 2) {
            if (heap[slot].less(candidate, cmp, workspace, stats)) {
                heap[slot].swap(candidate);
            }
        }
        heap[0] = candidate;
    }

    template<typename Compare>
    void PriorityQueueBase<Compare>::reset() {
        assert(isEmpty());
        for (int i = 0; i < capacity(); i++) {
            heap[i].key = LOW_SENTINEL(i);
        }
    }
}