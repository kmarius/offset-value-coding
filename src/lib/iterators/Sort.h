#pragma once

#include "lib/defs.h"
#include "lib/PriorityQueue.h"
#include "lib/Run.h"
#include "Iterator.h"
#include "lib/aggregates.h"

#include <vector>
#include <queue>

namespace ovc::iterators {

    template<bool DISTINCT, bool USE_OVC, typename Compare = RowCmpOVC, typename Aggregate = aggregates::Null, bool DO_AGGREGATE = false>
    struct Sorter {
        Compare cmp;
        Aggregate agg;
        Row *workspace;
        size_t workspace_size;
        std::vector<MemoryRun> memory_runs;
        std::queue<std::string> external_run_paths;
        std::vector<io::ExternalRunR> external_runs;
        io::BufferManager buffer_manager;
        PriorityQueue<USE_OVC, Compare> queue;
        Row prev;
        bool has_prev;
        iterator_stats *stats;

        explicit Sorter(iterator_stats *stats, const Compare &cmp, const Aggregate &agg = Aggregate());

        ~Sorter();

        void consume(Iterator *input);

        Row *next();

        void cleanup();

    private:
        inline bool equals(Row *row1, Row *row2) {
            if constexpr (USE_OVC) {
                return row2->key == 0;
            } else {
                return cmp(*row1, *row2) == 0;
            }
        }

        template<typename R>
        inline void process_row(Row *row, R &run) {
            if constexpr (DO_AGGREGATE) {
                // TODO: if we use OVCs, we don't need to check if the run is empty
                if (!run.isEmpty() && equals(run.back(), row)) {
                    agg.merge(*run.back(), *row);
                } else {
                    run.add(row);
                }
            } else if constexpr (DISTINCT) {
                if (run.isEmpty() || !equals(run.back(), row)) {
                    run.add(row);
                }
            } else {
                run.add(row);
            }
        }

        /**
         * Generate initial runs from the input. When this function returns, only in-memory runs are left in the queue
         * to be merged. Returns false if the input was empty and returned nullptr.
         * @return False if the input is empty.
         */
        bool generate_initial_runs(Iterator *input);

        /**
         * Merge all in-memory runs that reside in the queue. It is assumed that every item in the queue is an in-memory run.
         */
        void merge_in_memory();

        /**
         * Merge fan_in runs from the external_run_paths queue.
         * @param fan_in
         * @return The path of the new run.
         */
        std::string merge_external_runs(size_t fan_in);

        /**
         * Insert a number of external runs into the priority queue.
         * @param fan_in The number of runs to insert.
         * @return A vector of handles to the external runs.
         */
        void insert_external_runs(size_t fan_in);
    };

    template<bool DISTINCT, bool USE_OVC, typename Compare = RowCmpOVC>
    class SortBase : public UnaryIterator {
    public:
        SortBase(Iterator *input, const Compare &cmp)
                : UnaryIterator(input), sorter(&stats, cmp), count(0), stats_disabled(false) {
        };

        SortBase(Iterator *input) : SortBase(input, Compare(&stats)) {};

        void open() {
            Iterator::open();
            input->open();
            sorter.consume(input);
            input->close();
        }

        Row *next() {
            if (sorter.queue.isEmpty()) {
                return nullptr;
            }
            count++;
            return sorter.next();
        }

        void close() {
            Iterator::close();
            sorter.cleanup();
        }

        SortBase *disableStats(bool disable = true) {
            stats_disabled = disable;
            return this;
        }

        void accumulateStats(iterator_stats &acc) override {
            input->accumulateStats(acc);
            if (!stats_disabled) {
                acc.add(getStats());
            }
        }

        unsigned long getCount() const {
            return count;
        }

    private:
        Sorter<DISTINCT, USE_OVC, Compare> sorter;
        unsigned long count;
        bool stats_disabled;
    };

    typedef SortBase<false, false> SortNoOvc;

    class Sort : public SortBase<false, true, RowCmpOVC> {
    public:
        Sort(Iterator *input) : SortBase<false, true, RowCmpOVC>(input, RowCmpOVC(&this->stats)) {}
    };

    class SortPrefix : public SortBase<false, true, RowCmpPrefixOVC> {
    public:
        SortPrefix(Iterator *input, int sort_prefix)
                : SortBase<false, true, RowCmpPrefixOVC>(input, RowCmpPrefixOVC(sort_prefix, &this->stats)) {};
    };

    class SortDistinctPrefix : public SortBase<true, true, RowCmpPrefixOVC> {
    public:
        SortDistinctPrefix(Iterator *input, int sort_prefix)
                : SortBase<true, true, RowCmpPrefixOVC>(input, RowCmpPrefixOVC(sort_prefix, &this->stats)) {};
    };

    // TODO: doesnt work, investigate at some point
    class SortDistinctPrefixNoOVC : public SortBase<true, true, RowCmpPrefixNoOVC> {
    public:
        SortDistinctPrefixNoOVC(Iterator *input, int sort_prefix)
                : SortBase<true, true, RowCmpPrefixNoOVC>(input, RowCmpPrefixNoOVC(sort_prefix, &this->stats)) {};
    };

    class SortPrefixNoOVC : public SortBase<false, false, RowCmpPrefixNoOVC> {
    public:
        SortPrefixNoOVC(Iterator *input, int sort_prefix)
                : SortBase<false, false, RowCmpPrefixNoOVC>(input, RowCmpPrefixNoOVC(sort_prefix, &this->stats)) {};
    };
}

#include "Sort.ipp"