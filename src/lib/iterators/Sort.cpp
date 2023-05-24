#include "lib/Row.h"
#include "Sort.h"
#include "lib/PriorityQueue.h"
#include "lib/log.h"
#include "lib/io/ExternalRunW.h"
#include "lib/io/ExternalRunR.h"

#include <vector>
#include <algorithm>
#include <sstream>

#define IS_IN_WORKSPACE(ptr, ws) (ptr >= ws.begin().base() && ptr < ws.end().base())

static std::string new_run_path() {
    static int i = 0;
    return std::string(BASEDIR "/run_" + std::to_string(i++) + ".dat");
}

Sort::Sort(Iterator *input) : input(input), queue(QUEUE_CAPACITY), buffer_manager(1024), output(nullptr) {
    workspace.reserve(QUEUE_CAPACITY * QUEUE_CAPACITY);
};

Sort::~Sort() {
    assert(status == Closed);
    delete input;
};

// assume the priority queue is isEmpty and we are creating generate_initial_runs memory_runs
// output memory_runs are in-memory
// ends with queue filled with in-memory memory_runs
bool Sort::generate_initial_runs_q() {
    log_trace("Sort::generate_initial_runs()");

    memory_runs = {};
    memory_runs.reserve(QUEUE_CAPACITY);

    Row *row;
    for (size_t k = 0; k < QUEUE_CAPACITY - 3 && (row != nullptr); k++) {
        MemoryRun run;
        run.reserve(QUEUE_CAPACITY);

        for (; run.size() < QUEUE_CAPACITY && (row = input->next()) != nullptr;) {
            run.add(row);
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
bool Sort::generate_initial_runs() {
    log_trace("Sort::generate_initial_runs()");

    size_t runs_generated = 0;
    size_t rows_processed = 0;

    memory_runs = {};
    MemoryRun run;
    run.reserve(QUEUE_CAPACITY);
    memory_runs.reserve(QUEUE_CAPACITY);
    workspace.clear();

    Index insert_run_index = INITIAL_RUN_IDX;
    size_t inserted = 0;

    assert(queue.isCorrect());

    Row *row;

    for (; queue.size() < queue.capacity() && (row = input->next()) != nullptr;) {
        workspace.push_back(*row);
        row = &workspace.back();

        queue.push(row, insert_run_index);
        inserted++;
        rows_processed++;
    }

    assert(queue.isCorrect());

    if (inserted == 0) {
        return false;
    }

    if (inserted == QUEUE_CAPACITY) {
        insert_run_index++;
        inserted = 0;
    }

#ifndef NDEBUG
    Row prev = {0};
#endif

    for (; (row = input->next()) != nullptr;) {
        workspace.push_back(*row);
        row = &workspace.back();

        rows_processed++;
        Row *row1 = queue.pop_push(row, insert_run_index);
        run.add(row1);
        log_trace("popped %s, rows_processed=%lu", row1->c_str(), rows_processed);
        inserted++;
        assert(queue.isCorrect());

#ifndef NDEBUG
        if (run.size() == 1) {
            prev = {0};
        }
        if (row1->less(prev)) {
            log_error("prev: %s", prev.c_str());
            log_error("cur:  %s", row1->c_str());
        }
        assert(!row1->less(prev));
        prev = *row1;
#endif

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

    assert(queue.isCorrect());

    // finalize by inserting in-memory-memory_runs here
    size_t i;
    for (i = 0; i < memory_runs.size(); i++) {
        // replace top element with run
        Row *row1 = queue.pop_push_memory(&memory_runs[i]);
        run.add(row1);
        log_trace("final: %s", row1->c_str());
    }

    assert(queue.isCorrect());

    // we do not have enough memory_runs to replace the rows with, pop the rest
    for (; i < inserted; i++) {
        Row *row1 = queue.pop(MERGE_RUN_IDX);
        run.add(row1);
    }
    assert(queue.isCorrect());

    memory_runs.push_back(std::move(run));
    run = {};
    assert(memory_runs.back().isSorted());

    // insert the last run
    queue.push_memory(memory_runs.back());
    assert(queue.isCorrect());
    for (auto &r : memory_runs) {
        assert(r.isSorted());
    }

    runs_generated++;

    log_trace("generate_initial_runs: %lu memory_runs generated, last one has offset: %lu", runs_generated,
              memory_runs.back().size());
    log_trace("%lu rows processed", rows_processed);

    if (row == nullptr) {
        log_trace("Sort::generate_initial_runs(): input isEmpty");
    }
    return row != nullptr;
}

// assume: priority queue is filled with in-memory memory_runs
// output memory_runs are external
// ends with isEmpty priority queue
void Sort::merge_in_memory() {
    log_trace("Sort::merge_in_memory()");
    if (queue.isEmpty()) {
        return;
    }

    std::string path = new_run_path();
    ExternalRunW run(path, buffer_manager);

#ifndef NDEBUG
    Row prev;
#endif

    while (!queue.isEmpty()) {
        Row *row = queue.pop_memory();
        run.add(*row);
        assert(queue.isCorrect());

#ifndef NDEBUG
        if (run.size() > 1 && row->less(prev)) {
            log_error("Sort::merge_memory(): not ascending");
            log_error("Sort::merge_memory(): prev %s", prev.c_str());
            log_error("Sort::merge_memory(): cur  %s", row->c_str());
        }
        if (run.size() > 1) {
            assert(!row->less(prev));
        }
        prev = *row;
#endif
    }

    if (run.size() > 0) {
        log_trace("external run of offset %lu created in %s", run.size(), path.c_str());
        external_run_paths.push(path);
    }

    memory_runs.clear();
}

// priority queue is isEmpty
// output is an external run
// ends with isEmpty priority queue
void Sort::merge_external() {
    log_trace("Sort::merge_external()");

    assert(queue.isEmpty());
    assert(external_run_paths.size() > 1);

    std::vector<ExternalRunR> external_runs;
    external_runs.reserve(external_run_paths.size());

    int fan_in = 0;
    while (fan_in < queue.capacity() && !external_run_paths.empty()) {
        auto &path = external_run_paths.top();
        external_runs.emplace_back(path, buffer_manager);
        external_run_paths.pop();
        queue.push_external(external_runs.back());
    }

    std::string path = new_run_path();
    ExternalRunW run(path, buffer_manager);

#ifndef NDEBUG
    Row prev;
#endif

    while (!queue.isEmpty()) {
#ifndef NDEBUG
        const std::string &run_path = queue.top_path();
#endif
        Row *row = queue.pop_external();
        run.add(*row);

#ifndef NDEBUG
        if (run.size() > 1 && row->less(prev)) {
            log_error("Sort::merge_external(): not ascending in run %s", run_path.c_str());
            log_error("Sort::merge_external(): prev %s", prev.c_str());
            log_error("Sort::merge_external(): cur  %s", row->c_str());
        }
        if (run.size() > 1) {
            assert(!row->less(prev));
        }
        prev = *row;
#endif
    }

    for (auto &r: external_runs) {
        r.remove();
    }

    external_run_paths.push(path);

    log_trace("Sort::merge_external(): offset=%lu", run.size());
}

void Sort::open() {
    assert(status == Unopened);
    status = Opened;
    input->open();

    for (bool has_more_input = true; has_more_input;) {
        has_more_input = generate_initial_runs();
        merge_in_memory();
        assert(memory_runs.empty());
        // todo: add a counter, because  this actually merges runs of different levels
        if (external_run_paths.size() == queue.capacity()) {
            merge_external();
        }
    }
    while (external_run_paths.size() > 1) {
        merge_external();
    }
    if (!external_run_paths.empty()) {
        output = new ExternalRunR(external_run_paths.top(), buffer_manager);
    }
}

Row *Sort::next() {
    assert(status == Opened);
    if (output == nullptr) {
        return nullptr;
    }
    return output->read();
}

void Sort::free() {}

void Sort::close() {
    assert(status == Opened);
    status = Closed;
    if (output != nullptr) {
        output->remove();
        delete output;
        output = nullptr;
    }
    input->close();
}