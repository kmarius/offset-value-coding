#pragma once

#include "lib/defs.h"
#include "Iterator.h"
#include "lib/PriorityQueue.h"
#include "lib/Run.h"

#include <vector>

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
    std::stack<std::string, std::vector<std::string>> external_run_paths;
    PriorityQueue queue;
    ExternalRunR *output;
    BufferManager buffer_manager;

    bool generate_initial_runs_q();
    bool generate_initial_runs();
    void merge_in_memory();
    void merge_external();
};