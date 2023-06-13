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
#define NODE_KEY_BITS (sizeof(node_key_t) << 3)
#define NODE_RUN_IDX_BITS RUN_IDX_BITS
#define NODE_OFFSET_BITS 4

#define NODE_VALUE_BITS (NODE_KEY_BITS - NODE_RUN_IDX_BITS - NODE_OFFSET_BITS)
#define NODE_VALUE_MASK ((node_key_t) (((node_key_t) 1) << NODE_VALUE_BITS) - 1)
#define NODE_VALUE(key) ((key) & NODE_VALUE_MASK)

#define NODE_OFFSET_MASK (((((node_key_t) 1) << NODE_OFFSET_BITS) - 1) << NODE_VALUE_BITS)
#define NODE_OFFSET ((key) & NODE_OFFSET_MASK)

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

struct PriorityQueue::WorkspaceNode {
    Row *row;
    union {
        MemoryRun *memory_run;
        ExternalRunR *external_run;
    };

    WorkspaceNode() = default;

    WorkspaceNode(Row *row, MemoryRun *run) : row(row), memory_run(run) {};

    WorkspaceNode(Row *row, ExternalRunR *run) : row(row), external_run(run) {};

    explicit WorkspaceNode(Row *row) : row(row), memory_run(nullptr) {};

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
        if (IS_HIGH_SENTINEL(node.key)) {
            stream << ", run=" << node.value() << ", HI" << ":" << node.index << "]";
        } else {
            stream << ", run=" << node.run_index() << ", val=" << node.value() << ": ind=" << node.index << "]";
        }
        return stream;
    }

    inline bool isSibling(Node &node, const Index level) const {
        return index >> level == node.index >> level;
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

    inline void setOvc(OVC ovc) {
        key &= ~NODE_VALUE_MASK;
        key |= NODE_VALUE_MASK & ovc;
    }

    // sets ovc of the loser w.r.t. the winner
    inline bool less(Node &node, bool full_comp, WorkspaceNode *ws) {
        stats.comparisons++;
#ifdef PRIORITYQUEUE_USE_OVC
        if (full_comp || key == node.key) {
            stats.full_comparisons++;
            if (IS_HIGH_SENTINEL(key) || IS_HIGH_SENTINEL(node.key)) {
                return key < node.key;
            }
            if (run_index() != node.run_index()) {
                return key < node.key;
            }
            stats.actual_full_comparisons++;

            OVC ovc;
            // set key of the loser
            if (ws[index].row->less(*ws[node.index].row, ovc)) {
                node.setOvc(ovc);
                return true;
            } else {
                setOvc(ovc);
                return false;
            }
        }
        return key < node.key;
#else
        stats.full_comparisons++;
        if (IS_HIGH_SENTINEL(key) || IS_HIGH_SENTINEL(node.key)) {
            return key < node.key;
        }
        if (run_index() != node.run_index()) {
            return key < node.key;
        }
        stats.actual_full_comparisons++;

        OVC ovc;
        // set key of the loser
        if (ws[index].row->less(*ws[node.index].row, ovc)) {
            node.setOvc(ovc);
            return true;
        } else {
            setOvc(ovc);
            return false;
        }
#endif
    }
};

PriorityQueue::PriorityQueue(size_t capacity) : capacity_(capacity), size_(0), empty_slots() {
    assert(std::__popcount(capacity) == 1);

    workspace = new WorkspaceNode[capacity];
    heap = new Node[capacity];
    log_info("PriorityQueue capacity=%lu", capacity);
    for (int i = 0; i < capacity_; i++) {
        empty_slots.push(capacity_ - i - 1);
    }
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
    for (int l = 1; l < capacity_; l <<= 1) {
        size_t step = l << 1;
        size_t offset = capacity_ / step;
        for (int i = 0; i < capacity_ / step; i++) {
            heap[offset + i].index = l + i * step;
            heap[offset + i].key = HIGH_SENTINEL(l + i * step);
        }
    }
}

static inline void setMax(Key &x, Key const y) {
    if (x < y) {
        x = y;
    }
}

void PriorityQueue::pass(Index index, Key key, bool full_comp) {
    Node candidate(index, key);
    Index slot;
    Index level;

    for (slot = capacity_ / 2 + index / 2, level = 0; level++, slot != root() &&
                                                               heap[slot].index != index; slot = parent(slot)) {
        if (heap[slot].less(candidate, full_comp, workspace)) {
            heap[slot].swap(candidate);
            full_comp = false;
        }
    }

    Index dest = slot;
    if (candidate.index == index) {
        while (slot != root()) {
            const Index dest_level = level;
            do {
                slot = parent(slot);
                level++;
            } while (!heap[slot].isSibling(candidate, dest_level));

            if (heap[slot].less(candidate, full_comp, workspace)) {
                break;
            }

            heap[dest] = heap[slot];
            while (dest = parent(dest), dest != slot) {
                setMax(heap[dest].key, heap[slot].key);
            }
        }
    }

    heap[dest] = candidate;
}

void PriorityQueue::push(Row *row, Index run_index) {
    assert(size_ < capacity_);
    assert(!empty_slots.empty());

    Index ind = empty_slots.top();
    empty_slots.pop();

    log_trace("push %lu", ind);

    workspace[ind].row = row;
    pass(ind, NODE_KEYGEN(run_index, DOMAIN * ROW_ARITY + row->columns[0]), true);
    size_++;
}

void PriorityQueue::push_memory(MemoryRun &run) {
    assert(size_ < capacity_);
    assert(!empty_slots.empty());

    Index workspace_index = empty_slots.top();
    empty_slots.pop();

    log_trace("push_memory of size %lu at %lu, starting with %s", run.size(), workspace_index, run.front()->c_str());

    Row *row = run.front();

    workspace[workspace_index] = WorkspaceNode(row, &run);
    pass(workspace_index, NODE_KEYGEN(MERGE_RUN_IDX, row->key), true);
    size_++;
}

void PriorityQueue::push_external(ExternalRunR &run) {
    assert(size_ < capacity_);
    assert(!empty_slots.empty());

    Row *row = run.read();
    if (unlikely(row == nullptr)) {
        log_error("isEmpty run was pushed: %s", run.path().c_str());
    }
    assert(row != nullptr);

    Index workspace_index = empty_slots.top();
    empty_slots.pop();

    log_trace("push_external %lu", workspace_index);
    workspace[workspace_index] = WorkspaceNode(row, &run);
    pass(workspace_index, NODE_KEYGEN(MERGE_RUN_IDX, row->key), true);
    size_++;
}

Row *PriorityQueue::pop(Index run_index) {
    assert(!isEmpty());
    // pop lowest element
    Index workspace_index = heap[0].index;

    Row *res = workspace[heap[0].index].row;
    res->key = heap[0].ovc();

    // replace workspace item with high sentinel
    pass(workspace_index, HIGH_SENTINEL(run_index));
    empty_slots.push(workspace_index);

    size_--;

    log_trace("popped row at index %lu", workspace_index);

    return res;
}

Row *PriorityQueue::pop_push(Row *row, Index run_index) {
    assert(!isEmpty());
    Index workspace_index = heap[0].index;

    // pop lowest element and update its key
    Row *res = workspace[workspace_index].row;
    res->key = heap[0].ovc();

    workspace[workspace_index] = WorkspaceNode(row);

    // perform leaf-to-root pass
    pass(workspace_index, NODE_KEYGEN(run_index, DOMAIN * ROW_ARITY + row->columns[0]), true);

    return res;
}

Row *PriorityQueue::pop_push_memory(MemoryRun *run) {
    assert(!isEmpty());
    Index workspace_index = heap[0].index;

    // pop lowest element and update its key
    Row *res = workspace[workspace_index].row;
    res->key = heap[0].ovc();

    Row *row = run->front();
    workspace[workspace_index] = WorkspaceNode(row, run);

    // perform leaf-to-root pass
    pass(workspace_index, NODE_KEYGEN(MERGE_RUN_IDX, DOMAIN * ROW_ARITY + row->columns[0]), true);

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
        log_trace("pass %s", workspace[workspace_index].row->c_str());
        pass(workspace_index, NODE_KEYGEN(MERGE_RUN_IDX, workspace[workspace_index].row->key), true);
    } else {
        // last row, perform leaf-to-root with high fence
        workspace[workspace_index] = WorkspaceNode();
        pass(workspace_index, HIGH_SENTINEL(workspace_index), false);
        empty_slots.push(workspace_index);
        size_--;
        log_trace("in-memory run at position %lu isEmpty. queue_size: %lu", workspace_index, size());
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
        pass(workspace_index, NODE_KEYGEN(MERGE_RUN_IDX, workspace[workspace_index].row->key), true);
    } else {
        // last next from the run, perform leaf-to-root with high fence
        pass(workspace_index, HIGH_SENTINEL(MERGE_RUN_IDX));
        empty_slots.push(workspace_index);
        size_--;
        log_trace("external run at position %lu isEmpty. queue_size=%lu", workspace_index, size());
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
