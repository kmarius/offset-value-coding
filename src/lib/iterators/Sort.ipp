#pragma once

#include "lib/Row.h"
#include "Sort.h"
#include "lib/PriorityQueue.h"
#include "lib/log.h"
#include "lib/io/ExternalRunW.h"
#include "lib/io/ExternalRunR.h"
#include "lib/PriorityQueue.h"

#include <vector>
#include <sstream>

#define SORT_INITIAL_RUNS ((1 << RUN_IDX_BITS) - 3)
#define SORTER_WORKSPACE_CAPACITY (QUEUE_CAPACITY * SORT_INITIAL_RUNS)

namespace ovc::iterators {

    static std::string new_run_path() {
        static int i = 0;
        return std::string(BASEDIR "/run_" + std::to_string(i++) + ".dat");
    }

    template<bool DISTINCT, bool USE_OVC, typename Compare, typename Aggregate, bool DO_AGGREGATE>
    Sorter<DISTINCT, USE_OVC, Compare, Aggregate, DO_AGGREGATE>::Sorter(const Compare &cmp, const Aggregate &agg) :
            cmp(cmp),
            agg(agg),
            queue(QUEUE_CAPACITY, cmp),
            buffer_manager(1024),
            workspace(new Row[SORTER_WORKSPACE_CAPACITY]),
            workspace_size(0) {
    }

    template<bool DISTINCT, bool USE_OVC, typename Compare, typename Aggregate, bool DO_AGGREGATE>
    Sorter<DISTINCT, USE_OVC, Compare, Aggregate, DO_AGGREGATE>::~Sorter() {
        cleanup();
    };

    template<bool DISTINCT, bool USE_OVC, typename Compare, typename Aggregate, bool DO_AGGREGATE>
    void Sorter<DISTINCT, USE_OVC, Compare, Aggregate, DO_AGGREGATE>::cleanup() {
        delete[] workspace;
        workspace = nullptr;
        for (auto &run: external_runs) {
            run.remove();
        }
        external_runs.clear();
        while (!external_run_paths.empty()) {
            external_run_paths.pop();
        };
    }

    template<bool DISTINCT, bool USE_OVC, typename Compare, typename Aggregate, bool DO_AGGREGATE>
    bool Sorter<DISTINCT, USE_OVC, Compare, Aggregate, DO_AGGREGATE>::generate_initial_runs(Iterator *input) {
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
        queue.reset();

        Row *row;

        for (; queue.size() < queue.capacity() && (row = input->next());) {
            if (DO_AGGREGATE) {
                agg.init(*row);
            }
            workspace[workspace_size] = *row;
            input->free();
            row = &workspace[workspace_size++];
            row->key = MAKE_OVC(ROW_ARITY, 0, row->columns[0]);
            queue.push(row, insert_run_index);
            inserted++;
            rows_processed++;
        }

        queue.flush_sentinels();

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

        for (; (row = input->next());) {
            if (DO_AGGREGATE) {
                agg.init(*row);
            }
            workspace[workspace_size] = *row;
            input->free();
            row = &workspace[workspace_size++];
            row->key = MAKE_OVC(ROW_ARITY, 0, row->columns[0]);
#ifndef NDEBUG
            {
                if (run.isEmpty()) {
                    prev = {0};
                }
                Row *row_ = queue.top();
                if (cmp(*row_, prev) < 0) {
                    log_error("prev: %s", prev.c_str());
                    log_error("cur:  %s", row_->c_str());
                }
                assert(cmp(*row_, prev) >= 0);
                prev = *row_;
            }
#endif

            rows_processed++;
            Row *row1 = queue.pop_push(row, insert_run_index);
            process_row(row1, run);

            inserted++;
            assert(queue.isCorrect());

            if (inserted == QUEUE_CAPACITY) {
                assert(run.isSorted(cmp));
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
                Row *row1 = queue.pop_safe(MERGE_RUN_IDX);
                process_row(row1, run);
            }
        } else {
            // We filled the queue at least once. We must pop (QUEUE_CAPACITY - inserted) to fill the current run,
            // and then (inserted) to fill the remaining run
            size_t i = 0;
            size_t j = 0;
            for (; i < QUEUE_CAPACITY - inserted; i++) {
                if (j < memory_runs.size()) {
                    Row *row1 = queue.pop_push_memory(&memory_runs[j++]);
                    process_row(row1, run);
                } else {
                    Row *row1 = queue.pop_safe(MERGE_RUN_IDX);
                    process_row(row1, run);
                }
            }

            assert(queue.isCorrect());

            if (!run.isEmpty()) {
                memory_runs.push_back(std::move(run));
                run = {};
            }

            assert(queue.isCorrect());

            if (queue.size() == queue.capacity()) {
                Row *row1 = queue.pop(MERGE_RUN_IDX);
                process_row(row1, run);
                i++;
            }

            // we do not have enough memory_runs to replace the rows with, pop the rest
            for (; i < QUEUE_CAPACITY; i++) {
                if (j < memory_runs.size()) {
                    queue.push_memory(memory_runs[j++]);
                    Row *row1 = queue.pop_safe(MERGE_RUN_IDX);
                    process_row(row1, run);
                } else {
                    assert(queue.isCorrect());
                    Row *row1 = queue.pop_safe(MERGE_RUN_IDX);
                    process_row(row1, run);
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
            assert(r.isSorted(cmp));
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
    template<bool DISTINCT, bool USE_OVC, typename Compare, typename Aggregate, bool DO_AGGREGATE>
    void Sorter<DISTINCT, USE_OVC, Compare, Aggregate, DO_AGGREGATE>::merge_in_memory() {
        log_trace("SortBase::merge_in_memory()");
        if (queue.isEmpty()) {
            return;
        }

        std::string path = new_run_path();
        io::ExternalRunW run(path, buffer_manager);

#ifndef NDEBUG
        prev = {0};
#endif

        int i = 0;
        while (!queue.isEmpty()) {
            i++;
#ifndef NDEBUG
            {
                Row *row = queue.top();
                if (cmp(*row, prev) < 0) {
                    log_error("SortBase::merge_memory(): not ascending");
                    log_error("SortBase::merge_memory(): prev %s", prev.c_str());
                    log_error("SortBase::merge_memory(): cur  %s", row->c_str());
                }
                assert(cmp(*row, prev) >= 0);
                prev = *row;
            }
#endif
            Row *row1 = queue.pop_memory();
            if constexpr (DO_AGGREGATE) {
                while (!queue.isEmpty() && queue.top_ovc() == 0) {
                    agg.merge(*row1, *queue.top());
                    queue.pop_memory();
                }
                run.add(*row1);
            } else if constexpr (DISTINCT) {
                if constexpr (USE_OVC) {
                    run.add(*row1);
                    while (!queue.isEmpty() && queue.top_ovc() == 0) {
                        queue.pop_memory();
                    }
                } else {
                    run.add(*row1);
                    row1 = run.back();
                    while (!queue.isEmpty() && equals(row1, queue.top(), cmp)) {
                        queue.pop_memory();
                    }
                }
            } else {
                run.add(*row1);
            }
            assert(queue.isCorrect());
        }

        if (run.size() > 0) {
            log_trace("external run of offset %lu created in %s", run.size(), path.c_str());
            external_run_paths.push(path);
        }

        memory_runs.clear();
    }

    template<bool DISTINCT, bool USE_OVC, typename Compare, typename Aggregate, bool DO_AGGREGATE>
    void Sorter<DISTINCT, USE_OVC, Compare, Aggregate, DO_AGGREGATE>::insert_external_runs(size_t fan_in) {
        assert(fan_in <= QUEUE_CAPACITY);
        assert(queue.isEmpty());
        assert(external_run_paths.size() >= fan_in);

        queue.reset();

        external_runs.clear();
        external_runs.reserve(external_run_paths.size());

        for (size_t i = 0; i < fan_in; i++) {
            assert(!external_run_paths.empty());
            auto &path = external_run_paths.front();
            external_runs.emplace_back(path, buffer_manager);
            external_run_paths.pop();
            queue.push_external(external_runs.back());
        }
        queue.flush_sentinels();
    }

    template<bool DISTINCT, bool USE_OVC, typename Compare, typename Aggregate, bool DO_AGGREGATE>
    std::string Sorter<DISTINCT, USE_OVC, Compare, Aggregate, DO_AGGREGATE>::merge_external_runs(size_t fan_in) {
        log_trace("merging %lu external runs", QUEUE_CAPACITY);
        insert_external_runs(fan_in);

        std::string path = new_run_path();
        io::ExternalRunW run(path, buffer_manager);

#ifndef NDEBUG
        prev = {0};
#endif

        while (!queue.isEmpty()) {
#ifndef NDEBUG
            {
                Row *row_ = queue.top();
                const std::string &run_path = queue.top_path();
                if (cmp(*row_, prev) < 0) {
                    log_error("SortBase::merge_external(): not ascending in run %s", run_path.c_str());
                    log_error("SortBase::merge_external(): prev %s", prev.c_str());
                    log_error("SortBase::merge_external(): cur  %s", row_->c_str());
                }
                assert(cmp(*row_, prev) >= 0);
                prev = *row_;
            }
#endif
            Row *row = queue.pop_external();
            if constexpr (DO_AGGREGATE) {
                run.add(*row);
                row = run.back();
                while (!queue.isEmpty() && queue.top_ovc() == 0) {
                    agg.merge(*row, *queue.top());
                    queue.pop_external();
                }
            } else if constexpr (DISTINCT) {
                if constexpr (USE_OVC) {
                    run.add(*row);
                    while (!queue.isEmpty() && queue.top_ovc() == 0) {
                        queue.pop_external();
                    }
                } else {
                    run.add(*row);
                    row = run.back();
                    while (!queue.isEmpty() && equals(row, queue.top(), cmp)) {
                        queue.pop_external();
                    }
                }
            } else {
                run.add(*row);
            }
        }

        for (auto &r: external_runs) {
            r.remove();
        }
        external_runs.clear();

        external_run_paths.push(path);

        return path;
    }

    template<bool DISTINCT, bool USE_OVC, typename Compare, typename Aggregate, bool DO_AGGREGATE>
    void Sorter<DISTINCT, USE_OVC, Compare, Aggregate, DO_AGGREGATE>::consume(Iterator *input_) {
        for (bool has_more_input = true; has_more_input;) {
            has_more_input = generate_initial_runs(input_);
            merge_in_memory();
            assert(memory_runs.empty());
        }

        size_t num_runs = external_run_paths.size();
        if (num_runs > QUEUE_CAPACITY) {
            // this guarantees maximal fan-in for the later merges
            size_t initial_merge_fan_in = num_runs % (QUEUE_CAPACITY - 1);
            assert(initial_merge_fan_in > 0);
            merge_external_runs(initial_merge_fan_in);
        }

        while (external_run_paths.size() > QUEUE_CAPACITY) {
            merge_external_runs(QUEUE_CAPACITY);
        }

        insert_external_runs(external_run_paths.size());

#ifndef NDEBUG
        prev = {0};
#endif
        log_trace("Sorter::consume() fan-in for the last merge step is %lu", external_runs.size());
    }

    template<bool DISTINCT, bool USE_OVC, typename Compare, typename Aggregate, bool DO_AGGREGATE>
    Row *Sorter<DISTINCT, USE_OVC, Compare, Aggregate, DO_AGGREGATE>::next() {

#ifndef NDEBUG
        Row *row = nullptr;
        if constexpr (DISTINCT) {
            if constexpr (USE_OVC) {
                while ((row = queue.pop_external()) && row->key == 0) {
                    if (queue.isEmpty()) {
                        row = nullptr;
                        break;
                    }
                }
            } else {
                while ((row = queue.pop_external())) {
                    if (has_prev && row->equals(prev)) {
                        if (queue.isEmpty()) {
                            row = nullptr;
                            break;
                        }
                        continue;
                    }
                    break;
                }
            }
        } else {
            return queue.pop_external();
        }

        if (row == nullptr) {
            return nullptr;
        }

        if (cmp(*row, prev) < 0) {
            const std::string &run_path = queue.top_path();
            log_error("SortBase::merge_external(): not ascending in run %s", run_path.c_str());
            log_error("SortBase::merge_external(): prev %s", prev.c_str());
            log_error("SortBase::merge_external(): cur  %s", row->c_str());
        }
        assert(cmp(*row, prev) <= 0);

        has_prev = true;
        prev = *row;
        return row;
#else
        if constexpr (DO_AGGREGATE) {
            Row *row = queue.pop_external();
            while (!queue.isEmpty() && queue.top_ovc() == 0) {
                agg.merge(*queue.top(), *row);
                row = queue.pop_external();
            }
            agg.finalize(*row);
            return row;
        } else if constexpr (DISTINCT) {
            Row *row = nullptr;
            if constexpr (USE_OVC) {
                while ((row = queue.pop_external()) && row->key == 0) {
                    if (queue.isEmpty()) {
                        row = nullptr;
                        break;
                    }
                }
            } else {
                while ((row = queue.pop_external())) {
                    if (has_prev && row->equals(prev)) {
                        if (queue.isEmpty()) {
                            row = nullptr;
                            break;
                        }
                        continue;
                    }
                    has_prev = true;
                    prev = *row;
                    break;
                }
            }
            return row;
        } else {
            return queue.pop_external();
        }
#endif
    }
}