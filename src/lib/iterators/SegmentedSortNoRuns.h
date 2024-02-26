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

#define MERGE_RUN_IDX ((1ul << RUN_IDX_BITS) - 2)
#define INITIAL_RUN_IDX 1

namespace ovc::iterators {

    /*
     * Input: Rows sorted by A,B,C (optionally with OVCs)
     * Output: Rows sorted by A,C,B (optionally with OVCs)
     */
    template<typename EqualsA, typename EqualsB, typename Compare, size_t CAPACITY>
    struct SegmentedSorterNoRuns {
        EqualsA eqA;
        EqualsB eqB;
        Compare cmp;
        PriorityQueue<Compare> queue;
        iterator_stats *stats;
        std::vector<Row *> ptrs; // pointers to rows in insertion order
        Row *next_segment; // first row of the next segment once it is detected
        Row *prev;

        SegmentedSorterNoRuns(iterator_stats *stats, const EqualsA &eqA, const EqualsB &eqB, const Compare &cmp) :
                queue(CAPACITY, stats, cmp), stats(stats), eqA(eqA), eqB(eqB), cmp(cmp),
                next_segment(nullptr), prev(nullptr) {
            ptrs.reserve(1 << 21);

            assert(cmp.USES_OVC == eqA.USES_OVC);
            assert(cmp.USES_OVC == eqB.USES_OVC);
        }

        ~SegmentedSorterNoRuns() = default;

        void prep_next_segment(Iterator *input) {
            log_trace("prep_next_segment");

            assert(queue.isEmpty());
            ptrs.clear();

            Row * row;
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
            prev = row;

#ifdef COLLECT_STATS
            stats->segments_found++;
#endif

            unsigned long ovc = row->key;
            if constexpr (eqA.USES_OVC) {
                // Set new offset-value code with offset |A| and value from C[0]
                row->setNewOVC(eqA.arity, eqA.offset, eqA.columns[eqB.offset]);
            }

            ptrs.push_back(row);
            for (; (row = next_from_segment(input));) {
                ptrs.push_back(row);
            }
            assert(ptrs.size() <= CAPACITY);
            queue.reset(p2(ptrs.size()));
            for (auto ptr: ptrs) {
                queue.push(ptr, INITIAL_RUN_IDX);
            }
            queue.flush_sentinels();

            if constexpr (eqA.USES_OVC) {
                queue.top()->key = ovc;
            }
        }

        inline Row *next() {
            return queue.popf();
        }

    private:
        inline Row *next_from_segment(Iterator *input) {
            Row * row = input->next();
            if (!row) {
                return nullptr;
            }
            if constexpr (eqA.USES_OVC) {
                unsigned long offset = OVC_GET_OFFSET(row->key, ROW_ARITY);
                if (offset < eqA.offset) {
                    log_trace("segment boundary detected at row %s", row->c_str());
                    // Next segment, hold back the row
                    next_segment = row;
                    return nullptr;
                } else {
                    // Set new offset-value code with offset |A| and value from C[0]
                    row->setNewOVC(eqA.arity, eqA.offset, eqA.columns[eqB.offset]);
                }
            } else {
                if (!eqA(*row, *prev)) {
                    log_trace("segment boundary detected at row %s", row->c_str());
                    next_segment = row;
                    return nullptr;
                }
            }
            return row;
        }
    };

    template<typename EqualsA, typename EqualsB, typename Compare, size_t CAPACITY, bool SEGMENT>
    class SegmentedSortNoRunsBase : public UnaryIterator {
    public:
        SegmentedSortNoRunsBase(Iterator *input, const EqualsA &eqA, const EqualsB &eqB, const Compare &cmp)
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
        SegmentedSorterNoRuns<EqualsA, EqualsB, Compare, CAPACITY> sorter;
        bool input_empty;
    };

    using namespace comparators;

    template<size_t CAPACITY = QUEUE_CAPACITY>
    class SegmentedSortNoRuns
            : public SegmentedSortNoRunsBase<EqColumnList, EqColumnList, CmpColumnList, CAPACITY, true> {
    public:
        SegmentedSortNoRuns(Iterator *input,
                            uint8_t *columnsA, uint8_t lengthA,
                            uint8_t *columnsB, uint8_t lengthB,
                            uint8_t *columnsC, uint8_t lengthC)
                : SegmentedSortNoRunsBase<EqColumnList, EqColumnList, CmpColumnList, CAPACITY, true>(
                input,
                EqColumnList(columnsA, lengthA, &this->stats),
                EqColumnList(columnsB, lengthB, &this->stats),
                CmpColumnList(combine_lists(columnsC, lengthC, columnsB, lengthB), lengthC + lengthB, &this->stats)) {
        };
    };

    template<size_t CAPACITY = QUEUE_CAPACITY>
    class SegmentedSortNoRunsOVC
            : public SegmentedSortNoRunsBase<EqOffset, EqOffset, CmpColumnListOVC, CAPACITY, true> {
    public:
        SegmentedSortNoRunsOVC(Iterator *input,
                               uint8_t *columnsABC, uint8_t lengthA, uint8_t lengthB, uint8_t lengthC)
                : SegmentedSortNoRunsBase<EqOffset, EqOffset, CmpColumnListOVC, CAPACITY, true>(
                input,
                EqOffset(columnsABC, lengthA + lengthB + lengthC, lengthA, &this->stats),
                EqOffset(columnsABC, lengthA + lengthB + lengthC, lengthA + lengthB, &this->stats),
                CmpColumnListOVC(transpose_cb(columnsABC, lengthA, lengthB, lengthC), lengthA + lengthB + lengthC,
                                 &this->stats)
        ) {};
    };
}