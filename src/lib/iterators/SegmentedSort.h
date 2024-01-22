#pragma once

#include <queue>
#include "Iterator.h"
#include "lib/io/BufferManager.h"
#include "lib/PriorityQueue.h"

#define SSORT_INITIAL_RUNS ((1 << RUN_IDX_BITS) - 3)
#define SSORTER_WORKSPACE_CAPACITY (QUEUE_CAPACITY * SSORT_INITIAL_RUNS)

namespace ovc::iterators {

    template<typename Compare, typename Equals>
    struct SegmentedSorter {
        Compare cmp;
        Equals eq;
        std::vector<Row> workspace;
        std::queue<MemoryRun> memory_runs;
        std::vector<MemoryRun> current_merge_runs;
        PriorityQueue<Compare> queue;
        iterator_stats *stats;
        Row *prev;
        Row *next_segment;

        SegmentedSorter(iterator_stats *stats, const Compare &cmp, const Equals &eq) :
                queue(QUEUE_CAPACITY, stats, cmp), eq(eq), cmp(cmp), prev(nullptr), next_segment(nullptr) {


            workspace.reserve(1000);
        }

        ~SegmentedSorter() {
        };

        void insert_memory_runs(size_t num_runs) {
            log_trace("insert_memory_runs %lu", num_runs);
            assert(num_runs > 0);
            assert(queue.isEmpty());
            queue.reset();
            current_merge_runs.clear();
            current_merge_runs.reserve(num_runs);
            for (size_t i = 0; i < num_runs; i++) {
                MemoryRun run = memory_runs.front();
                memory_runs.pop();
                current_merge_runs.push_back(run);
                queue.push_memory(current_merge_runs.back());
            }
            queue.flush_sentinels();
            assert(!queue.isEmpty());
        }

        void merge_memory_runs(size_t num_runs) {
            log_trace("merge_memory_runs %lu", num_runs);
            assert(num_runs > 0);
            insert_memory_runs(num_runs);
            MemoryRun run;
            while (!queue.isEmpty()) {
                run.add(queue.pop_memory());
            }
            log_trace("memory run size: %lu", run.size());
            memory_runs.push(run);
        }

        void prepNextSegment(Iterator *input) {
            log_trace("prepNextSegment");
            sortNextSegment(input);
            assert(queue.isEmpty());

            size_t num_runs = memory_runs.size();

            if (num_runs == 0) {
                return;
            }

            if (num_runs > QUEUE_CAPACITY) {
                // this guarantees maximal fan-in for the later merges
                size_t initial_merge_fan_in = num_runs % (QUEUE_CAPACITY - 1);
                assert(initial_merge_fan_in > 0);
                merge_memory_runs(initial_merge_fan_in);
            }

            while (memory_runs.size() > QUEUE_CAPACITY) {
                merge_memory_runs(QUEUE_CAPACITY);
            }

            insert_memory_runs(memory_runs.size());
        }

        void sortNextSegment(Iterator *input) {
            assert(queue.isEmpty());
            queue.reset();

            // read rows until we find the next segment and hold it back

            Row *row;
            MemoryRun run;

reset:
            // fill up queue once
            for (int i = 0; i < QUEUE_CAPACITY && (row = nextFromSegment(input)); i++) {
                queue.push(row, INITIAL_RUN_IDX);
            }

            if (!row) {
                // queue didn't fill up once

                if (queue.isEmpty()) {
                    // no input
                    return;
                }

                while (!queue.isEmpty()) {
                    run.add(queue.pop_safe(MERGE_RUN_IDX));
                }

                memory_runs.push(run);
                return;
            }

            auto current_run_idx = queue.top_run_idx();

            for (;;) {
                auto top_run_idx = queue.top_run_idx();
                if (top_run_idx != current_run_idx) {
                    log_trace("new memory run");
                    memory_runs.push(run);
                    run = {};
                    current_run_idx++;
                }

                if (current_run_idx == MERGE_RUN_IDX) {
                    log_trace("flushing queue");
                    while (!queue.isEmpty()) {
                        run.add(queue.pop_safe(MERGE_RUN_IDX));
                    }
                    memory_runs.push(run);
                    queue.reset();
                    run = {};
                    goto reset;
                } else {
                    row = nextFromSegment(input);
                    log_trace("next from segment is %s", row ? row->c_str() : "null");
                    if (!row) {
                        // segment ended
                        break;
                    }
                    run.add(queue.pop_push(row, current_run_idx + 1));
                }
            }

            while (!queue.isEmpty()) {
                queue.flush_sentinel(true);
                auto top_run_idx = queue.top_run_idx();
                if (top_run_idx != current_run_idx) {
                    log_trace("new memory run");
                    memory_runs.push(run);
                    run = {};
                    current_run_idx++;
                }
                run.add(queue.pop_safe(MERGE_RUN_IDX));
            }

            if (!run.isEmpty()) {
                memory_runs.push(run);
            }

            queue.reset();
        }

        Row *next() {
            return queue.pop_memory();
        }

    private:
        Row *nextFromSegment(Iterator *input) {
            log_trace("nextFromSegment");
            if (next_segment) {
                prev = next_segment;
                next_segment = nullptr;
                return prev;
            }
            Row *row = input->next();
            if (!row) {
                log_trace("input empty");
                // input empty
                return nullptr;
            }
            log_trace("%s", row->c_str());
            if (!prev) {
                // very first call
                workspace.push_back(*row);
                prev = &workspace.back();
                return prev;
            } else {
                workspace.push_back(*row);
                row = &workspace.back();

                if (eq(*prev, *row)) {
                    // same segment
                    prev = row;
                    return row;
                } else {
                    log_trace("segment boundary detected at row %s", row->c_str());
                    // next segment, hold back the row and return null
                    next_segment = row;
                    prev = nullptr;
                    return nullptr;
                }
            }
        }
    };

    template<typename Compare, typename Equals>
    class SegmentedSortBase : public UnaryIterator {
    public:
        SegmentedSortBase(Iterator *input, const Compare &cmp, const Equals &eq)
                : UnaryIterator(input), sorter(&stats, cmp, eq), input_empty(false) {
        };

        void open() override {
            Iterator::open();
            input->open();
        }

        Row *next() override {
            Iterator::next();
            if (sorter.queue.isEmpty()) {
                if (input_empty) {
                    return nullptr;
                }

                sorter.prepNextSegment(input);

                if (sorter.queue.isEmpty()) {
                    input_empty = true;
                    return nullptr;
                }
            }
            return sorter.next();
        }

        void free() override {
            Iterator::free();
        }

        void close() override {
            Iterator::close();
            input->close();
        }

    private:
        SegmentedSorter<Compare, Equals> sorter;
        bool input_empty;
    };

    template<typename Compare, typename Equals>
    class SegmentedSort : public SegmentedSortBase<Compare, Equals> {
    public:
        SegmentedSort(Iterator *input, const Compare &cmp, const Equals &eq)
                : SegmentedSortBase<Compare, Equals>(input, cmp, eq) {
            assert(cmp.USES_OVC == eq.USES_OVC);
        };
    };
}

#include "SegmentedSort.ipp"