#pragma once

#include "lib/defs.h"
#include "Iterator.h"
#include "lib/PriorityQueue.h"
#include "lib/Run.h"

#include <vector>
#include <queue>

class Sort : public Iterator {
public:
    explicit Sort(Iterator *input);

    ~Sort() override;

    void open() override;

    Row *next() override;

    void free() override;

    void close() override;

private:
    Iterator *input;
    std::vector<Row> workspace;
    std::vector<MemoryRun> memory_runs;
    std::queue<std::string> external_run_paths;
    std::vector<ExternalRunR> external_runs;
    PriorityQueue queue;
    BufferManager buffer_manager;

    bool generate_initial_runs_q();
    bool generate_initial_runs();
    void merge_in_memory();

    /**
     * Merge fan_in runs from the external_run_paths queue.
     * @param fan_in
     * @return The path of the new run.
     */
    std::string merge_external(size_t fan_in);

    std::vector<ExternalRunR> insert_external(size_t fan_in);


#ifndef NDEBUG
    Row prev = {0};
#endif
};