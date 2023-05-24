#pragma once

#include "lib/defs.h"
#include "lib/Row.h"
#include "BufferManager.h"

#include <cstdint>
#include <cstddef>
#include <cassert>
#include <cstring>
#include <liburing.h>
#include <stdexcept>

class ExternalRunW {
private:
    BufferManager *buffer_manager;
    Buffer *buffers[2];

    std::string path_;
    int fd;

    size_t sizes[2];
    bool ready[2];

    io_uring ring;

    // the buffer currently being written to
    int current;

    // Number of rows in the current buffer
    size_t rows;

    // Total number of rows written
    size_t rows_total;

    // add offset in the file
    size_t offset;

    void flush(int index);

    void submit_write(int index);

    void wait_for_write_completion(int index);

    void write_to_buffer(void *data, size_t size);

public:
    explicit ExternalRunW(const std::string &path, BufferManager &buffer_manager);

    ~ExternalRunW();

    /**
     * Add a row to this run.
     * @param row
     */
    void add(Row &row);

    /**
     * Get the number of rows that have already been written to this run.
     * @return The number of rows.
     */
    size_t size() const;

    /**
     * Get the path_sync of the file.
     * @return The path_sync of the file.
     */
    const std::string &path() const;

    /**
     * Flush all buffer and close the file. This function is automatically called on destruction.
     */
    void finalize();
};