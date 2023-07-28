#pragma once

#include "lib/defs.h"
#include "lib/PriorityQueue.h"
#include "lib/Run.h"
#include "Iterator.h"
#include "lib/Aggregates.h"

#include <vector>
#include <queue>

namespace ovc::iterators {

    const bool DistinctOff = false;
    const bool DistinctOn = true;

    const bool OvcOff = false;
    const bool OvcOn = true;

    template<bool DISTINCT, bool USE_OVC, typename Compare = RowCmp, typename Aggregate = NullAggregate, bool DO_AGGREGATE = false>
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

        explicit Sorter(const Compare &cmp = Compare(), const Aggregate &agg = Aggregate());

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

    template<bool DISTINCT, bool USE_OVC, typename Compare = RowCmp>
    class SortBase : public UnaryIterator {
    public:
        SortBase(Iterator *input, const Compare &cmp = Compare())
                : UnaryIterator(input), sorter(cmp), count(0) {
            output_has_ovc = USE_OVC;
            output_is_sorted = true;
            output_is_unique = DISTINCT;
        };

        void open() {
            Iterator::open();
            input_->open();
            sorter.consume(input_);
            input_->close();
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

        struct ovc_stats &getStats() {
            return sorter.queue.getStats();
        }

        unsigned long getCount() const {
            return count;
        }

    private:
        Sorter<DISTINCT, USE_OVC, Compare> sorter;
        unsigned long count;
    };

    typedef SortBase<DistinctOff, OvcOn> Sort;
    typedef SortBase<DistinctOn, OvcOn> SortDistinct;
    typedef SortBase<DistinctOff, OvcOff> SortNoOvc;
    typedef SortBase<DistinctOn, OvcOff> SortDistinctNoOvc;
}

#include "Sort.ipp"