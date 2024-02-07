#pragma once

#include <queue>
#include "Iterator.h"
#include "lib/io/BufferManager.h"
#include "lib/PriorityQueue.h"

namespace ovc::iterators {

    /*
     * Input: Rows sorted by A,B,C (optionally with OVCs)
     * Output: Rows sorted by A,C,B (optionally with OVCs)
     */
    template<typename EqualsA, typename EqualsB, typename Compare>
    struct SegmentedSorter {
        EqualsA eqA;
        EqualsB eqB;
        Compare cmp;
        std::vector<Row> workspace;
        std::queue<MemoryRun> memory_runs;
        std::vector<MemoryRun> current_merge_runs;
        PriorityQueue<Compare> queue;
        iterator_stats *stats;
        Row *prev;
        Row *next_segment;
        std::vector<OVC> stored_ovcs;

        SegmentedSorter(iterator_stats *stats, const EqualsA &eqA, const EqualsB &eqB, const Compare &cmp) :
                queue(QUEUE_CAPACITY, stats, cmp), stats(stats), eqA(eqA), eqB(eqB), cmp(cmp), prev(nullptr),
                next_segment(nullptr) {
            workspace.reserve(1 << 20);

            assert(cmp.USES_OVC == eqA.USES_OVC);
            assert(cmp.USES_OVC == eqB.USES_OVC);
        }

        ~SegmentedSorter() {};

        void prep_next_segment(Iterator *input) {
            log_trace("prep_next_segment");

            assert(queue.isEmpty());
            stored_ovcs.clear();
            sort_next_segment(input);
            assert(queue.isEmpty());

            if constexpr (eqA.USES_OVC) {
                queue.cmp.stored_ovcs = &stored_ovcs[0];
                for (auto ovc: stored_ovcs) {
                    log_trace("%lu@%lu", OVC_FMT(ovc));
                }
            }

            size_t num_runs = memory_runs.size();

            if (num_runs == 0) {
                return;
            }

            if (num_runs > QUEUE_CAPACITY) {
                assert(false); // disabled for now

                // this guarantees maximal fan-in for the later merges
                size_t initial_merge_fan_in = num_runs % (QUEUE_CAPACITY - 1);
                if (initial_merge_fan_in == 0) {
                    initial_merge_fan_in = QUEUE_CAPACITY - 1;
                }
                merge_memory_runs(initial_merge_fan_in);
            }

            while (memory_runs.size() > QUEUE_CAPACITY) {
                merge_memory_runs(QUEUE_CAPACITY);
            }

            insert_memory_runs(memory_runs.size());

            if constexpr (eqA.USES_OVC) {
                // Restore OVC for the first row in the segment
                assert(stored_ovcs.size() > 0);
                queue.top()->key = stored_ovcs[0];
            }
        }

        inline Row *next() {
            return queue.pop_memory();
        }

    private:
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
            memory_runs.push(run);
        }

        void sort_next_segment(Iterator *input) {
            assert(queue.isEmpty());
            assert(memory_runs.empty());
            queue.reset();

            // Read rows from the segment (until next_from_segment returns null
            // Rows with same B go to the same run
            // when input is exhausted, merge the runs on attributes CB

            MemoryRun run;

            Row *row = next_from_segment(input);
            if (!row) {
                // no more input
                return;
            }

            if constexpr (eqA.USES_OVC) {
                // First row in run, store its offset-value code
                stored_ovcs.push_back(row->key);

                // Set new offset-value code with offset |A| and value from C[0]
                row->setNewOVC(eqA.arity, eqA.offset, eqA.columns[eqB.offset]);
            }

            long run_index = 0;

            row->tid = run_index;
            run.add(row);

            Row *prev = row;

            for (; (row = next_from_segment(input));) {
                if (!eqB(*row, *prev)) {
                    // Change in B detected, create a new run
                    memory_runs.push(run);
                    run = {};
                    run_index++;
                    if constexpr (eqA.USES_OVC) {
                        // First row in run, store its offset-value code
                        stored_ovcs.push_back(row->key);

                        // Set new offset-value code with offset |A| and value C[0]
                        row->setNewOVC(eqA.arity, eqA.offset, eqA.columns[eqB.offset]);
                    }
                } else {
                    if constexpr (eqA.USES_OVC) {
                        // Unless the row is a dupe (indicated by a code of 0), decrement the offset-value code by |B|
                        if (row->key) {
                            auto offset = row->getOffset();
                            row->setNewOVC(eqA.arity, offset - (eqB.offset - eqA.offset), eqA.columns[offset]);
                        }
                    }
                }
                row->tid = run_index;
                run.add(row);
                prev = row;
            }

            if (!run.isEmpty()) {
                memory_runs.push(run);
            }
        }

        Row *next_from_segment(Iterator *input) {
            if (next_segment) {
                prev = next_segment;
                next_segment = nullptr;
                return prev;
            }
            Row *row = input->next();
            if (!row) {
                log_trace("next_from_segment: input empty");
                // input empty
                return nullptr;
            }
            if (!prev) {
                // very first call in segment
                workspace.push_back(*row);
                prev = &workspace.back();
                return prev;
            } else {
                workspace.push_back(*row);
                row = &workspace.back();

                if (eqA(*row, *prev)) {
                    // same segment
                    prev = row;
                    return row;
                } else {
                    log_trace("next_from_segment: segment boundary detected at row %s", row->c_str());
                    // Next segment, hold back the row and return null
                    next_segment = row;
                    prev = nullptr;
                    return nullptr;
                }
            }
        }
    };

    template<typename EqualsA, typename EqualsB, typename Compare>
    class SegmentedSortBase : public UnaryIterator {
    public:
        SegmentedSortBase(Iterator *input, const EqualsA &eqA, const EqualsB &eqB, const Compare &cmp)
                : UnaryIterator(input), sorter(&stats, eqA, eqB, cmp), input_empty(false) {
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

                sorter.prep_next_segment(input);

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
        SegmentedSorter<EqualsA, EqualsB, Compare> sorter;
        bool input_empty;
    };

    using namespace comparators;

    class SegmentedSort : public SegmentedSortBase<EqColumnList, EqColumnList, CmpColumnListCool> {
    public:
        SegmentedSort(Iterator *input,
                      uint8_t *columnsA, uint8_t lengthA,
                      uint8_t *columnsB, uint8_t lengthB,
                      uint8_t *columnsC, uint8_t lengthC)
                : SegmentedSortBase<EqColumnList, EqColumnList, CmpColumnListCool>(
                input,
                EqColumnList(columnsA, lengthA, &this->stats),
                EqColumnList(columnsB, lengthB, &this->stats),
                CmpColumnListCool(columnsC, lengthC, &this->stats).append(columnsB, lengthB)) {
        };
    };

    class SegmentedSortOVC : public SegmentedSortBase<EqOffset, EqOffset, CmpColumnListDerivingOVC> {
    public:
        SegmentedSortOVC(Iterator *input,
                         uint8_t *columnsABC, uint8_t lengthA, uint8_t lengthB, uint8_t lengthC)
                : SegmentedSortBase<EqOffset, EqOffset, CmpColumnListDerivingOVC>(
                input,
                EqOffset(columnsABC, lengthA + lengthB + lengthC, lengthA, &this->stats),
                EqOffset(columnsABC, lengthA + lengthB + lengthC, lengthA + lengthB, &this->stats),
                CmpColumnListDerivingOVC(columnsABC, lengthA, lengthB, lengthC, &this->stats).transposeBC()
        ) {};
    };
}