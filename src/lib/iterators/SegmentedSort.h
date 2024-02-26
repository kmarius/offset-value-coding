#pragma once

#include "Iterator.h"
#include "lib/io/BufferManager.h"
#include "lib/PriorityQueue.h"
#include "lib/utils.h"

#include <queue>
#include <tuple>

/*
 * CAUTION:
 * This currently is not a valid ONC operator because it doesn't copy rows into its own memory to sort them, but assumes
 * they exist for the whole lifetime of a segment. A RowBuffer iterator can be used before this one to buffer rows.
 */

namespace ovc::iterators {

    /*
     * Input: Rows sorted by A,B,C (optionally with OVCs)
     * Output: Rows sorted by A,C,B (optionally with OVCs)
     */
    template<typename EqualsA, typename EqualsB, typename Compare, size_t CAPACITY>
    struct SegmentedSorter {
        EqualsA eqA;
        EqualsB eqB;
        Compare cmp;
        PriorityQueue<Compare> queue;
        iterator_stats *stats;
        std::vector<Row *> ptrs; // pointers to rows in insertion order
        std::vector<std::tuple<Row **, Row **>> runs; // (begin, end) of runs point into the ptrs array
        Row *next_segment; // first row of the next segment once it is detected
        std::vector<OVC> stored_ovcs; // original ovcs of the first row in each run; reset for each segment

        SegmentedSorter(iterator_stats *stats, const EqualsA &eqA, const EqualsB &eqB, const Compare &cmp) :
                queue(CAPACITY, stats, cmp), stats(stats), eqA(eqA), eqB(eqB), cmp(cmp),
                next_segment(nullptr) {
            ptrs.reserve(1 << 21);

            assert(cmp.USES_OVC == eqA.USES_OVC);
            assert(cmp.USES_OVC == eqB.USES_OVC);
        }

        ~SegmentedSorter() = default;

        void prep_next_segment(Iterator *input) {
            log_trace("prep_next_segment");

            assert(queue.isEmpty());
            stored_ovcs.clear();
            runs.clear();
            ptrs.clear();
            process_next_segment(input);
            assert(queue.isEmpty());

            if constexpr (eqA.USES_OVC) {
                queue.cmp.stored_ovcs = &stored_ovcs[0];
                for (auto ovc: stored_ovcs) {
                    log_trace("%lu@%lu", OVC_FMT(ovc));
                }
            }

            size_t num_runs = runs.size();

            if (num_runs == 0) {
                return;
            }

            if (num_runs > CAPACITY) {
                assert(false); // disabled for now
            }

            insert_memory_runs(runs.size());

            if constexpr (eqA.USES_OVC) {
                // Restore OVC for the first row in the segment
                assert(!stored_ovcs.empty());
                queue.top()->key = stored_ovcs[0];
            }
        }

        // treat everything as one large segment, generating runs for constant values of AB
        void prep_next_unsegment(Iterator *input) {
            log_trace("prep_next_unsegment");

            assert(queue.isEmpty());
            stored_ovcs.clear();
            runs.clear();
            ptrs.clear();
            sort_next_unsegment(input);
            assert(queue.isEmpty());

            if constexpr (eqA.USES_OVC) {
                //queue.cmp.stored_ovcs = &stored_ovcs[0];
            }

            size_t num_runs = runs.size();

            if (num_runs == 0) {
                return;
            }

            if (num_runs > CAPACITY) {
                assert(false); // disabled for now
            }

            insert_memory_runs(runs.size());

            if constexpr (eqA.USES_OVC) {
                // Restore OVC for the first row in the segment
                assert(!stored_ovcs.empty());
                queue.top()->key = stored_ovcs[0];
            }
        }

        inline Row *next() {
            return queue.pop_memory2();
        }

    private:
        void insert_memory_runs(size_t num_runs) {
            log_trace("insert_memory_runs %lu", num_runs);

            assert(num_runs > 0);
            assert(queue.isEmpty());

            uint64_t next_p2 = p2(num_runs);
            if (next_p2 != queue.getCapacity()) {
                queue.reset(next_p2);
            } else {
                queue.reset();
            }

            for (size_t i = 0; i < num_runs; i++) {
                queue.push_memory2(&runs[i]);
            }
            queue.flush_sentinels();
            assert(!queue.isEmpty());
        }

        // processes the next segment in the input
        void process_next_segment(Iterator *input) {
            assert(queue.isEmpty());
            assert(runs.empty());

            // Read rows from the segment (until next_from_segment returns null
            // Rows with same B go to the same run
            // when input is exhausted, merge the runs on attributes CB

            Row *row;
            if (next_segment) {
                row = next_segment;
                next_segment = nullptr;
            } else {
                row = input->next();
                if (!row) {
                    // no more input
                    return;
                }
            }

#ifdef COLLECT_STATS
            stats->segments_found++;
#endif

            if constexpr (eqA.USES_OVC) {
                // First row in run, store its offset-value code
                stored_ovcs.push_back(row->key);

                // Set new offset-value code with offset |A| and value from C[0]
                row->setNewOVC(eqA.arity, eqA.offset, eqA.columns[eqB.offset]);
            }

            long run_index = 0;
            row->tid = run_index;

            assert(ptrs.empty());
            Row **run_first = &ptrs[0];
            size_t run_length = 0;

            ptrs.push_back(row);
            run_length++;

            for (Row *prev = row; (row = input->next()); prev = row) {
                if constexpr (eqA.USES_OVC) {
                    unsigned long ovc = row->key;
                    unsigned long offset = OVC_GET_OFFSET(ovc, ROW_ARITY);
                    if (offset < eqA.offset) {
                        // Next segment, hold back the row
                        log_trace("segment boundary detected at row %s", row->c_str());
                        next_segment = row;
                        break;
                    } else if (offset < eqB.offset) {
                        // Change in B detected, create a new run
                        runs.emplace_back(run_first, run_first + run_length);
                        run_first = &ptrs[ptrs.size()];
                        run_length = 0;
                        run_index++;
#ifdef COLLECT_STATS
                        stats->runs_generated++;
#endif
                        // First row in run, store its offset-value code
                        stored_ovcs.push_back(ovc);

                        // Set new offset-value code with offset |A| and value C[0]
                        row->setNewOVC(eqA.arity, eqA.offset, eqA.columns[eqB.offset]);
                    } else {
                        // same run, unless the row is a dupe (indicated by a code of 0), decrement the offset-value code by |B|
                        if (row->key) {
                            row->setNewOVC(eqA.arity, offset - (eqB.offset - eqA.offset), eqA.columns[offset]);
                        }
                    }
                } else {
                    if (!eqA(*row, *prev)) {
                        // Next segment
                        next_segment = row;
                        log_trace("segment boundary detected at row %s", row->c_str());
                        break;
                    } else if (!eqB(*row, *prev)) {
                        // Change in B detected, create a new run
                        runs.emplace_back(run_first, run_first + run_length);
                        run_first = &ptrs[ptrs.size()];
                        run_length = 0;
                        run_index++;
#ifdef COLLECT_STATS
                        stats->runs_generated++;
#endif
                    }
                }
                row->tid = run_index;
                ptrs.push_back(row);
                run_length++;
            }

            if (run_length > 0) {
                runs.emplace_back(run_first, run_first + run_length);
#ifdef COLLECT_STATS
                stats->runs_generated++;
#endif
            }
        }

        // whole input is one large segment
        void sort_next_unsegment(Iterator *input) {
            assert(queue.isEmpty());
            assert(runs.empty());

            Row *row = input->next();
            if (!row) {
                return;
            }

#ifdef COLLECT_STATS
            stats->segments_found++;
#endif

            if constexpr (eqA.USES_OVC) {
                stored_ovcs.push_back(row->key);
                row->setNewOVC(eqA.arity, 0, eqA.columns[0]);
            }

            long run_index = 0;
            row->tid = run_index;

            assert(ptrs.empty());
            Row **run_first = &ptrs[0];
            size_t run_length = 0;

            ptrs.push_back(row);
            run_length++;

            for (Row *prev = row; (row = input->next()); prev = row) {
                if constexpr (eqA.USES_OVC) {
                    unsigned long ovc = row->key;
                    unsigned long offset = OVC_GET_OFFSET(ovc, ROW_ARITY);
                    if (offset < eqB.offset) {
                        // Change in AB detected, create a new run
                        runs.emplace_back(run_first, run_first + run_length);
                        run_first = &ptrs[ptrs.size()];
                        run_length = 0;
                        run_index++;
#ifdef COLLECT_STATS
                        stats->runs_generated++;
#endif
                        stored_ovcs.push_back(ovc);
                        row->setNewOVC(eqA.arity, 0, eqA.columns[0]);
                    } else {
                        // same run, unless the row is a dupe (indicated by a code of 0), decrement the offset-value code by |B|
                        if (row->key) {
                            row->setNewOVC(eqA.arity, offset - (eqB.offset - eqA.offset), eqA.columns[offset]);
                        }
                    }
                } else {
                    if (!eqA(*row, *prev) || !eqB(*row, *prev)) {
                        // Change in AB detected, create a new run
                        runs.emplace_back(run_first, run_first + run_length);
                        run_first = &ptrs[ptrs.size()];
                        run_length = 0;
                        run_index++;
#ifdef COLLECT_STATS
                        stats->runs_generated++;
#endif
                    }
                }
                row->tid = run_index;
                ptrs.push_back(row);
                run_length++;
            }

            if (run_length > 0) {
                runs.emplace_back(run_first, run_first + run_length);
#ifdef COLLECT_STATS
                stats->runs_generated++;
#endif
            }
            log_trace("unsegmented: %lu runs created", runs.size());
        }
    };

    template<typename EqualsA, typename EqualsB, typename Compare, size_t CAPACITY, bool SEGMENT>
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

                if constexpr (SEGMENT) {
                    sorter.prep_next_segment(input);
                } else {
                    sorter.prep_next_unsegment(input);
                }

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
        SegmentedSorter<EqualsA, EqualsB, Compare, CAPACITY> sorter;
        bool input_empty;
    };

    using namespace comparators;

    template<size_t CAPACITY = QUEUE_CAPACITY>
    class SegmentedSort : public SegmentedSortBase<EqColumnList, EqColumnList, CmpColumnListCool, CAPACITY, true> {
    public:
        SegmentedSort(Iterator *input,
                      uint8_t *columnsA, uint8_t lengthA,
                      uint8_t *columnsB, uint8_t lengthB,
                      uint8_t *columnsC, uint8_t lengthC)
                : SegmentedSortBase<EqColumnList, EqColumnList, CmpColumnListCool, CAPACITY, true>(
                input,
                EqColumnList(columnsA, lengthA, &this->stats),
                EqColumnList(columnsB, lengthB, &this->stats),
                CmpColumnListCool(columnsC, lengthC, &this->stats).append(columnsB, lengthB)) {
        };
    };

    template<size_t CAPACITY = QUEUE_CAPACITY>
    class SegmentedSortOVC : public SegmentedSortBase<EqOffset, EqOffset, CmpColumnListDerivingOVC, CAPACITY, true> {
    public:
        SegmentedSortOVC(Iterator *input,
                         uint8_t *columnsABC, uint8_t lengthA, uint8_t lengthB, uint8_t lengthC)
                : SegmentedSortBase<EqOffset, EqOffset, CmpColumnListDerivingOVC, CAPACITY, true>(
                input,
                EqOffset(columnsABC, lengthA + lengthB + lengthC, lengthA, &this->stats),
                EqOffset(columnsABC, lengthA + lengthB + lengthC, lengthA + lengthB, &this->stats),
                CmpColumnListDerivingOVC(columnsABC, lengthA, lengthB, lengthC, &this->stats).transposeBC()
        ) {};
    };

    // Unsegmented variants merely exist to investigate the effects of segmenting

    template<size_t CAPACITY = QUEUE_CAPACITY>
    class UnSegmentedSort : public SegmentedSortBase<EqColumnList, EqColumnList, CmpColumnListCool, CAPACITY, false> {
    public:
        UnSegmentedSort(Iterator *input,
                        uint8_t *columnsA, uint8_t lengthA,
                        uint8_t *columnsB, uint8_t lengthB,
                        uint8_t *columnsC, uint8_t lengthC)
                : SegmentedSortBase<EqColumnList, EqColumnList, CmpColumnListCool, CAPACITY, false>(
                input,
                EqColumnList(columnsA, lengthA, &this->stats),
                EqColumnList(columnsB, lengthB, &this->stats),
                CmpColumnListCool(combine_lists(columnsA, lengthA, columnsC, lengthC), lengthA + lengthC,
                                  &this->stats).append(columnsB, lengthB)) {
        };
    };

    template<size_t CAPACITY = QUEUE_CAPACITY>
    class UnSegmentedSortOVC : public SegmentedSortBase<EqOffset, EqOffset, CmpColumnListOVC, CAPACITY, false> {
    public:
        UnSegmentedSortOVC(Iterator *input,
                           uint8_t *columnsABC, uint8_t lengthA, uint8_t lengthB, uint8_t lengthC)
                : SegmentedSortBase<EqOffset, EqOffset, CmpColumnListOVC, CAPACITY, false>(
                input,
                EqOffset(columnsABC, lengthA + lengthB + lengthC, lengthA, &this->stats),
                EqOffset(columnsABC, lengthA + lengthB + lengthC, lengthA + lengthB, &this->stats),
                //CmpColumnListDerivingOVC(columnsABC, lengthA, lengthB, lengthC, &this->stats).transposeBC()
                CmpColumnListOVC(transpose_cb(columnsABC, lengthA, lengthB, lengthC), lengthA+ lengthB+ lengthC, &this->stats)
        ) {};
    };
}