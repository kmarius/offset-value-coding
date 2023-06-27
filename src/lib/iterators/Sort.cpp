#include "lib/Row.h"
#include "Sort.h"
#include "lib/PriorityQueue.h"
#include "lib/log.h"
#include "lib/io/ExternalRunW.h"
#include "lib/io/ExternalRunR.h"

#include <vector>
#include <sstream>

static std::string new_run_path() {
    static int i = 0;
    return std::string(BASEDIR "/run_" + std::to_string(i++) + ".dat");
}

template<bool DISTINCT>
SortBase<DISTINCT>::SortBase(Iterator *input) : UnaryIterator(input),
                                                queue(QUEUE_CAPACITY),
                                                buffer_manager(1024),
                                                workspace(new Row[QUEUE_CAPACITY * (QUEUE_CAPACITY - 3)]),
                                                workspace_size(0) {
};

template<bool DISTINCT>
SortBase<DISTINCT>::~SortBase() {
    delete[] workspace;
};

// assume the priority queue is isEmpty and we are creating generate_initial_runs memory_runs
// output memory_runs are in-memory
// ends with queue filled with in-memory memory_runs
template<bool DISTINCT>
bool SortBase<DISTINCT>::generate_initial_runs_q() {
    log_trace("SortBase::generate_initial_runs()");

    memory_runs = {};
    memory_runs.reserve(QUEUE_CAPACITY);

    Row *row;
    for (size_t k = 0; k < QUEUE_CAPACITY - 3 && (row != nullptr); k++) {
        MemoryRun run;
        run.reserve(QUEUE_CAPACITY);

        for (; run.size() < QUEUE_CAPACITY && (row = UnaryIterator::next());) {
            run.add(row);
            UnaryIterator::free();
        }
        run.sort();
        run.setOvcs();
        memory_runs.push_back(run);
    }

    // fill up queue
    for (auto &run: memory_runs) {
        queue.push_memory(run);
    }

    return row != nullptr;
}

// assume the priority queue is isEmpty and we are creating generate_initial_runs memory_runs
// output memory_runs are in-memory
// ends with queue filled with in-memory memory_runs
template<bool DISTINCT>
bool SortBase<DISTINCT>::generate_initial_runs() {
    log_trace("SortBase::generate_initial_runs()");

    size_t runs_generated = 0;
    size_t rows_processed = 0;

    memory_runs = {};
    MemoryRun run;
    run.reserve(QUEUE_CAPACITY);
    memory_runs.reserve(QUEUE_CAPACITY);
    workspace_size = 0;

    Index insert_run_index = INITIAL_RUN_IDX;
    size_t inserted = 0;

    assert(queue.isCorrect());

    Row *row;

    for (; queue.size() < queue.capacity() && (row = UnaryIterator::next());) {
        workspace[workspace_size] = *row;
        UnaryIterator::free();
        row = &workspace[workspace_size++];

        queue.push(row, insert_run_index);
        inserted++;
        rows_processed++;
    }

    assert(queue.isCorrect());

    if (inserted == 0) {
        log_trace("SortBase::generate_initial_runs(): input empty");
        return false;
    }

    if (inserted == QUEUE_CAPACITY) {
        insert_run_index++;
        inserted = 0;
    }

#ifndef NDEBUG
    prev = {0};
#endif

    for (; (row = UnaryIterator::next());) {
        workspace[workspace_size] = *row;
        UnaryIterator::free();
        row = &workspace[workspace_size++];

#ifndef NDEBUG
        {
            if (run.isEmpty()) {
                prev = {0};
            }
            Row *row_ = queue.top();
            if (row_->less(prev)) {
                log_error("prev: %s", prev.c_str());
                log_error("cur:  %s", row_->c_str());
            }
            assert(!row_->less(prev));
            prev = *row_;
        }
#endif

        rows_processed++;
        Row *row1 = queue.pop_push(row, insert_run_index);
        if (!DISTINCT || row1->key != 0) {
            run.add(row1);
        }

        inserted++;
        assert(queue.isCorrect());

        if (inserted == QUEUE_CAPACITY) {
            assert(run.isSorted());
            memory_runs.push_back(std::move(run));
            run = {};
            insert_run_index++;
            runs_generated++;

            if (insert_run_index == MERGE_RUN_IDX) {
                break;
            }

            inserted = 0;
        }
    }

    assert(queue.isCorrect());

    if (inserted == 0) {
        if (!queue.isEmpty()) {
            inserted = QUEUE_CAPACITY;
        }
    }

    if (queue.size() < queue.capacity()) {
        // The queue is not even full once. Pop everything, we then have a single in-memory run.
        while (!queue.isEmpty()) {
            log_trace("pop run_idx=%lu", queue.top_run_idx());
            Row *row1 = queue.pop(MERGE_RUN_IDX);
            if (!DISTINCT || row1->key != 0) {
                run.add(row1);
            }
        }
    } else {
        // We filled the queue at least once. We must pop (QUEUE_CAPACITY - inserted) to fill the current run,
        // and then (inserted) to fill the remaining run
        size_t i = 0;
        size_t j = 0;
        for (; i < QUEUE_CAPACITY - inserted; i++) {
            log_trace("run_idx=%lu", queue.top_run_idx());
            if (j < memory_runs.size()) {
                Row *row1 = queue.pop_push_memory(&memory_runs[j++]);
                if (!DISTINCT || row1->key != 0) {
                    run.add(row1);
                }
            } else {
                Row *row1 = queue.pop(MERGE_RUN_IDX);
                if (!DISTINCT || row1->key != 0) {
                    run.add(row1);
                }
            }
        }

        assert(queue.isCorrect());

        if (!run.isEmpty()) {
            memory_runs.push_back(std::move(run));
            run = {};
        }

        // we do not have enough memory_runs to replace the rows with, pop the rest
        for (; i < QUEUE_CAPACITY; i++) {
            if (j < memory_runs.size()) {
                Row *row1 = queue.pop_push_memory(&memory_runs[j++]);
                if (!DISTINCT || row1->key != 0) {
                    run.add(row1);
                }
            } else {
                Row *row1 = queue.pop(MERGE_RUN_IDX);
                if (!DISTINCT || row1->key != 0) {
                    run.add(row1);
                }
            }
        }
    }

    assert(queue.isCorrect());

    if (!run.isEmpty()) {
        memory_runs.push_back(std::move(run));
        run = {};
    }

    // insert the last run
    queue.push_memory(memory_runs.back());

    assert(queue.isCorrect());
    for (auto &r: memory_runs) {
        assert(r.size() > 0);
        assert(r.isSorted());
    }

    runs_generated++;

    log_trace("generate_initial_runs: %lu memory_runs generated, last one has offset: %lu", runs_generated,
              memory_runs.back().size());
    log_trace("%lu rows processed", rows_processed);

    if (row == nullptr) {
        log_trace("SortBase::generate_initial_runs(): input empty");
    }
    return row != nullptr;
}

// assume: priority queue is filled with in-memory memory_runs
// output memory_runs are external
// ends with isEmpty priority queue
template<bool DISTINCT>
void SortBase<DISTINCT>::merge_in_memory() {
    log_trace("SortBase::merge_in_memory()");
    if (queue.isEmpty()) {
        return;
    }

    std::string path = new_run_path();
    ExternalRunW run(path, buffer_manager);

#ifndef NDEBUG
    prev = {0};
#endif

    while (!queue.isEmpty()) {
#ifndef NDEBUG
        {
            Row *row = queue.top();
            if (row->less(prev)) {
                log_error("SortBase::merge_memory(): not ascending");
                log_error("SortBase::merge_memory(): prev %s", prev.c_str());
                log_error("SortBase::merge_memory(): cur  %s", row->c_str());
            }
            assert(!row->less(prev));
            prev = *row;
        }
#endif
        Row *row = queue.pop_memory();
        if (!DISTINCT || row->key != 0) {
            run.add(*row);
        }
        assert(queue.isCorrect());
    }

    if (run.size() > 0) {
        log_trace("external run of offset %lu created in %s", run.size(), path.c_str());
        external_run_paths.push(path);
    }

    memory_runs.clear();
}

template<bool DISTINCT>
std::vector<ExternalRunR> SortBase<DISTINCT>::insert_external(size_t fan_in) {
    assert(fan_in <= QUEUE_CAPACITY);
    assert(queue.isEmpty());
    assert(external_run_paths.size() >= fan_in);

    std::vector<ExternalRunR> external_runs;
    external_runs.reserve(external_run_paths.size());

    for (size_t i = 0; i < fan_in; i++) {
        assert(!external_run_paths.empty());
        auto &path = external_run_paths.front();
        external_runs.emplace_back(path, buffer_manager);
        external_run_paths.pop();
        queue.push_external(external_runs.back());
    }
    return external_runs;
}

// priority queue is isEmpty
// output is an external run
// ends with isEmpty priority queue
template<bool DISTINCT>
std::string SortBase<DISTINCT>::merge_external(size_t fan_in) {
    log_trace("SortBase::merge_external()");
    external_runs = insert_external(fan_in);

    std::string path = new_run_path();
    ExternalRunW run(path, buffer_manager);

#ifndef NDEBUG
    prev = {0};
#endif

    while (!queue.isEmpty()) {
#ifndef NDEBUG
        {
            Row *row_ = queue.top();
            const std::string &run_path = queue.top_path();
            if (row_->less(prev)) {
                log_error("SortBase::merge_external(): not ascending in run %s", run_path.c_str());
                log_error("SortBase::merge_external(): prev %s", prev.c_str());
                log_error("SortBase::merge_external(): cur  %s", row_->c_str());
            }
            assert(!row_->less(prev));
            prev = *row_;
        }
#endif
        Row *row = queue.pop_external();
        if (!DISTINCT || row->key != 0) {
            run.add(*row);
        }
    }

    for (auto &r: external_runs) {
        r.remove();
    }
    external_runs.clear();

    external_run_paths.push(path);

    log_trace("SortBase::merge_external(): size=%lu", run.size());

    return path;
}

template<bool DISTINCT>
void SortBase<DISTINCT>::open() {
    UnaryIterator::open();

    for (bool has_more_input = true; has_more_input;) {
        has_more_input = generate_initial_runs();
        merge_in_memory();
        assert(memory_runs.empty());
    }

    size_t num_runs = external_run_paths.size();
    if (num_runs > QUEUE_CAPACITY) {
        // this guarantees maximal fan-in for the later merges
        size_t initial_merge_fan_in = num_runs % (QUEUE_CAPACITY - 1);
        assert(initial_merge_fan_in);
        log_trace("merging %lu external runs", initial_merge_fan_in);
        merge_external(initial_merge_fan_in);
    }

    while (external_run_paths.size() > QUEUE_CAPACITY) {
        log_trace("merging %lu external runs", QUEUE_CAPACITY);
        merge_external(QUEUE_CAPACITY);
    }

    external_runs = insert_external(external_run_paths.size());

#ifndef NDEBUG
    prev = {0};
#endif
    log_trace("SortBase::open() fan-in for the last merge step is %lu", external_runs.size());
}

template<bool DISTINCT>
Row *SortBase<DISTINCT>::next() {
    if (queue.isEmpty()) {
        return nullptr;
    }

#ifndef NDEBUG
    Row *row;
    if (DISTINCT) {
        while ((row = queue.pop_external()) && row->key == 0) {}
    } else {
        row = queue.pop_external();
    }

    if (row == nullptr) {
        return nullptr;
    }

    if (row->less(prev)) {
        const std::string &run_path = queue.top_path();
        log_error("SortBase::merge_external(): not ascending in run %s", run_path.c_str());
        log_error("SortBase::merge_external(): prev %s", prev.c_str());
        log_error("SortBase::merge_external(): cur  %s", row->c_str());
    }
    assert(!row->less(prev));

    prev = *row;
    return row;
#else
    if (DISTINCT) {
        Row *row;
        while ((row = queue.pop_external()) && row->key == 0) {}
        return row;
    } else {
        return queue.pop_external();
    }
#endif
}

template<bool DISTINCT>
void SortBase<DISTINCT>::close() {
    UnaryIterator::close();
    for (auto &run: external_runs) {
        run.remove();
    }
}

template
class SortBase<false>;

template
class SortBase<true>;