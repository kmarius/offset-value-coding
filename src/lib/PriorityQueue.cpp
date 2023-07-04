#include <sstream>
#include "PriorityQueue.h"

/*
 * comparison key, numbers of bits:
 *
 * +------------+--------+-------+
 * |  run_index | offset | value |
 * +------------+--------+-------+
 * |     10     |    4   |   48  |
 * +------------+--------+-------+
 *
 * run2 index 0 are low sentinel values, run2 index 1024 high sentinel values
 *
 */

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
#define NODE_OFFSET(key) ((key) & NODE_OFFSET_MASK)

#define NODE_OVC_MASK (NODE_OFFSET_MASK|NODE_VALUE_MASK)
#define NODE_OVC(key) ((key) & (NODE_OVC_MASK))

#define NODE_RUN_INDEX_MASK (((((node_key_t) 1) << NODE_RUN_IDX_BITS) - 1) << (NODE_OFFSET_BITS + NODE_VALUE_BITS))
#define NODE_RUN_INDEX(key) ((key & NODE_RUN_INDEX_MASK) >> (NODE_OFFSET_BITS + NODE_VALUE_BITS))

#define NODE_KEYGEN(run_index, key) ((((node_key_t) run_index) << (NODE_OFFSET_BITS + NODE_VALUE_BITS)) | key)

#define IS_LOW_SENTINEL(key) (NODE_RUN_INDEX(key) == 0)
#define IS_HIGH_SENTINEL(key) (((key) & NODE_RUN_INDEX_MASK) == NODE_RUN_INDEX_MASK)
#define LOW_SENTINEL(run_index) (NODE_KEYGEN(0, run_index))
#define HIGH_SENTINEL(run_index) (NODE_RUN_INDEX_MASK | (run_index))

struct priority_queue_stats stats;

struct PriorityQueue::WorkspaceItem {
    Row *row;
    union {
        MemoryRun *memory_run;
        ExternalRunR *external_run;
    };

    WorkspaceItem() = default;

    WorkspaceItem(Row *row, MemoryRun *run) : row(row), memory_run(run) {};

    WorkspaceItem(Row *row, ExternalRunR *run) : row(row), external_run(run) {};

    explicit WorkspaceItem(Row *row) : row(row), memory_run(nullptr) {};

    void pop() {
        assert(memory_run->size() > 0);
        memory_run->next();
        row = memory_run->front();
    }
};

struct PriorityQueue::Node {
    /* Sort key in this priority queue. */
    node_key_t key;

    /* Index of the corresponding element in the workspace (i.e. row, run2) */
    node_index_t index;

    Node() : key(0), index(0) {}

    Node(Index index, Key key) : key(key), index(index) {}

    friend std::ostream &operator<<(std::ostream &stream, const Node &node) {
        stream << "[key=" << node.key;
        if (IS_LOW_SENTINEL(node.key)) {
            stream << ", run=" << node.value() << ", LO" << ":" << node.index << "]";
        } else if (IS_HIGH_SENTINEL(node.key)) {
            stream << ", run=" << node.value() << ", HI" << ":" << node.index << "]";
        } else {
            stream << ", run=" << node.run_index() << ", val=" << node.value() << ": ind=" << node.index << "]";
        }
        return stream;
    }

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

    inline void setOvc(OVC ovc) {
        key &= ~NODE_VALUE_MASK;
        key |= NODE_VALUE_MASK & ovc;
    }

    // sets ovc of the loser w.r.t. the winner
    inline bool less(Node &node, WorkspaceItem *ws, struct ovc_stats *ovc_stats = nullptr) {
        stats.comparisons++;

#ifdef PRIORITYQUEUE_NO_USE_OVC
        stats.comparisons_equal_key++;
        if (!isValid() || !node.isValid() || run_index() != node.run_index()) {
            return key < node.key;
        }
        stats.comparisons_of_actual_rows++;

        OVC ovc;
        return ws[index].row->less(*ws[node.index].row, ovc, ovc_stats);
#else
        if (key == node.key) {
            stats.comparisons_equal_key++;
            if (!isValid() || !node.isValid()) {
                return false;
            }
            stats.comparisons_of_actual_rows++;

            OVC ovc;
            if (ws[index].row->less(*ws[node.index].row, ovc, NODE_OFFSET(key) + 1, ovc_stats)) {
                node.setOvc(ovc);
                return true;
            } else {
                setOvc(ovc);
                return false;
            }
        }

        return key < node.key;
#endif
    }
};

PriorityQueue::PriorityQueue(size_t capacity) : capacity_(capacity), size_(0) {
    assert(std::__popcount(capacity) == 1);

    workspace = new WorkspaceItem[capacity];
    heap = new Node[capacity];
    log_info("PriorityQueue capacity=%lu", capacity);
    reset();
}

PriorityQueue::~PriorityQueue() {
    delete[] workspace;
    delete[] heap;
}

size_t PriorityQueue::size() const {
    return size_;
}

bool PriorityQueue::isEmpty() const {
    return size_ == 0;
}

size_t PriorityQueue::capacity() const {
    return capacity_;
}

bool PriorityQueue::isCorrect() const {
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

void PriorityQueue::reset() {
    assert(isEmpty());
    heap[0].index = 0;
    heap[0].key = LOW_SENTINEL(0);
    for (int l = 1; l < capacity_; l <<= 1) {
        size_t step = l << 1;
        size_t offset = capacity_ / step;
        for (int i = 0; i < capacity_ / step; i++) {
            heap[offset + i].index = l + i * step;
            heap[offset + i].key = LOW_SENTINEL(0);
        }
    }
}

static inline void setMax(Key &x, Key const y) {
    if (x < y) {
        x = y;
    }
}

void PriorityQueue::pass(Index index, Key key) {
    Node candidate(index, key);
    for (Index slot = capacity_ / 2 + index / 2; slot != 0; slot /= 2) {
        if (heap[slot].less(candidate, workspace, &ovc_stats)) {
            heap[slot].swap(candidate);
        }
    }
    heap[0] = candidate;
}

void PriorityQueue::push(Row *row, Index run_index) {
    assert(size_ < capacity_);
    assert(heap[0].isLowSentinel());

    Index workspace_index = heap[0].index;

    workspace[workspace_index].row = row;
    pass(workspace_index, NODE_KEYGEN(run_index, MAKE_OVC(ROW_ARITY, 0, row->columns[0])));
    size_++;
}

void PriorityQueue::push_memory(MemoryRun &run) {
    assert(size_ < capacity_);
    assert(heap[0].isLowSentinel());

    Index workspace_index = heap[0].index;

    log_trace("push_memory of size %lu at %lu, starting with %s", run.size(), workspace_index, run.front()->c_str());

    Row *row = run.front();

    workspace[workspace_index] = WorkspaceItem(row, &run);
    pass(workspace_index, NODE_KEYGEN(MERGE_RUN_IDX, row->key));
    size_++;
}

void PriorityQueue::push_external(ExternalRunR &run) {
    assert(size_ < capacity_);
    assert(heap[0].isLowSentinel());

    Row *row = run.read();
    if (unlikely(row == nullptr)) {
        log_error("isEmpty run was pushed: %s", run.path().c_str());
    }
    assert(row != nullptr);

    Index workspace_index = heap[0].index;

    log_trace("push_external %lu", workspace_index);
    workspace[workspace_index] = WorkspaceItem(row, &run);
    pass(workspace_index, NODE_KEYGEN(MERGE_RUN_IDX, row->key));
    size_++;
}

Row *PriorityQueue::pop(Index run_index) {
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

Row *PriorityQueue::pop_push(Row *row, Index run_index) {
    assert(!isEmpty());
    Index workspace_index = heap[0].index;

    // pop lowest element and update its key
    Row *res = workspace[workspace_index].row;
    res->key = heap[0].ovc();

    workspace[workspace_index] = WorkspaceItem(row);

    // perform leaf-to-root pass
    pass(workspace_index, NODE_KEYGEN(run_index, MAKE_OVC(ROW_ARITY, 0, row->columns[0])));

    return res;
}

Row *PriorityQueue::pop_push_memory(MemoryRun *run) {
    assert(!isEmpty());
    Index workspace_index = heap[0].index;

    // pop lowest element and update its key
    Row *res = workspace[workspace_index].row;
    res->key = heap[0].ovc();

    Row *row = run->front();
    workspace[workspace_index] = WorkspaceItem(row, run);

    // perform leaf-to-root pass
    pass(workspace_index, NODE_KEYGEN(MERGE_RUN_IDX, MAKE_OVC(ROW_ARITY, 0, row->columns[0])));

    return res;
}

Row *PriorityQueue::pop_memory() {
    assert(!isEmpty());
    Index workspace_index = heap[0].index;
    assert(workspace[workspace_index].memory_run->size() > 0);

    Row *res = workspace[workspace_index].row;
    res->key = heap[0].ovc();

    workspace[workspace_index].pop();

    if (likely(workspace[workspace_index].memory_run->size() > 0)) {
        // normal leaf-to-root pass
        pass(workspace_index, NODE_KEYGEN(MERGE_RUN_IDX, workspace[workspace_index].row->key));
    } else {
        // last row, perform leaf-to-root with high fence
        workspace[workspace_index] = WorkspaceItem();
        pass(workspace_index, HIGH_SENTINEL(MERGE_RUN_IDX));
        size_--;
        log_trace("in-memory run at position %lu empty. queue_size: %lu", workspace_index, size());
    }

    return res;
}

Row *PriorityQueue::pop_external() {
    assert(!isEmpty());
    Index workspace_index = heap[0].index;

    Row *res = workspace[workspace_index].row;
    res->key = heap[0].ovc();

    ExternalRunR *run = workspace[workspace_index].external_run;
    Row *next = run->read();
    workspace[workspace_index].row = next;

    if (likely(next != nullptr)) {
        // normal leaf-to-root pass
        pass(workspace_index, NODE_KEYGEN(MERGE_RUN_IDX, workspace[workspace_index].row->key));
    } else {
        // last next from the run, perform leaf-to-root with high fence
        pass(workspace_index, HIGH_SENTINEL(MERGE_RUN_IDX));
        size_--;
        log_trace("external run at position %lu empty. queue_size=%lu", workspace_index, size());
    }
    return res;
}

std::ostream &operator<<(std::ostream &stream, const PriorityQueue &pq) {
    for (size_t i = 0; i < pq.capacity_; i++) {
        if (i > 0) {
            stream << std::endl;
        }
        stream << "(slot=" << i << " " << pq.heap[i];
        if (pq.heap[i].isValid()) {
            stream << " " << *pq.workspace[pq.heap[i].index].row << ")";
        }
    }
    return stream;
}

void priority_queue_stats_reset() {
    memset(&stats, 0, sizeof stats);
}

Row *PriorityQueue::top() {
    assert(!isEmpty());
    return workspace[heap[0].index].row;
}

std::string PriorityQueue::to_string() const {
    std::stringstream stream;
    stream << *this << std::endl << "";
    return stream.str();
}

const std::string &PriorityQueue::top_path() {
    assert(!isEmpty());
    return workspace[heap[0].index].external_run->path();
}

size_t PriorityQueue::top_run_idx() {
    return heap[0].run_index();
}

void PriorityQueue::flush_sentinels() {
    for (int i = size_; i < capacity_; i++) {
        Index workspace_index = heap[0].index;
        pass(workspace_index, HIGH_SENTINEL(MERGE_RUN_IDX));
    }
}

void PriorityQueue::flush_sentinel(bool safe) {
    if (safe) {
        if (heap[0].isLowSentinel()) {
            pass(heap[0].index, HIGH_SENTINEL(MERGE_RUN_IDX));
        }
    } else {
        assert(heap[0].isLowSentinel());
        pass(heap[0].index, HIGH_SENTINEL(MERGE_RUN_IDX));
    }
}

Row *PriorityQueue::pop_safe(Index run_index) {
    if (heap[0].isLowSentinel()) {
        flush_sentinel();
    }
    return pop(run_index);
}
