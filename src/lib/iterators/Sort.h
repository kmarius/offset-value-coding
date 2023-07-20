#pragma once

#include "lib/defs.h"
#include "lib/PriorityQueue.h"
#include "lib/Run.h"
#include "Iterator.h"
#include "UnaryIterator.h"

#include <vector>
#include <queue>

namespace ovc::iterators {

    const bool DistinctOff = false;
    const bool DistinctOn = true;

    const bool OvcOff = false;
    const bool OvcOn = true;

    template<bool DISTINCT, bool USE_OVC, typename Compare = RowCmp>
    struct Sorter {
        Compare cmp;
        Row *workspace;
        size_t workspace_size;
        std::vector<MemoryRun> memory_runs;
        std::queue<std::string> external_run_paths;
        std::vector<io::ExternalRunR> external_runs;
        io::BufferManager buffer_manager;
        PriorityQueue<USE_OVC, Compare> queue;
        Row prev;
        bool has_prev;

        explicit Sorter(const Compare &cmp = Compare());

        ~Sorter();

        void consume(Iterator *input);

        void cleanup();

    private:
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
        explicit SortBase(Iterator *input, const Compare &cmp = Compare());

        void open() override;

        Row *next() override;

        void free() override {
            Iterator::free();
        }

        void close() override;

        struct ovc_stats &getStats() {
            return sorter.queue.getStats();
        }

    private:
        Sorter<DISTINCT, USE_OVC, Compare> sorter;
        Compare cmp;
    };

    typedef SortBase<DistinctOff, OvcOn> Sort;
    typedef SortBase<DistinctOn, OvcOn> SortDistinct;
    typedef SortBase<DistinctOff, OvcOff> SortNoOvc;
    typedef SortBase<DistinctOn, OvcOff> SortDistinctNoOvc;
}

#include "Sort.ipp"