#pragma once

#include "Buffer.h"

#include <liburing.h>
#include <stdexcept>
#include <map>
#include <cassert>

// TODO: we need to raise the file descriptor limit if we actually want to merge 1024 memory_runs
#define MAX_DESCRIPTORS 1024
#define PAGES_MAX 1024

class BufferManager {
private:
    // io_uring handle
    io_uring ring;

    // Maps file descriptors to buffer for files that are loading, or have been loaded but not collected
    // if a read "fails" (because there is nothing left to next), maps to nullptr.
    std::map<int, Buffer *> loading;

    // completed[fd] == true iff the last next has completed (successfull or not). The buffer is retrieved from
    // the loading map
    bool completed[MAX_DESCRIPTORS];

    size_t capacity;
    uint16_t free[PAGES_MAX];
    uint16_t free_ptr;
    uint8_t *buffers_raw;
    Buffer *buffers_aligned;

public:
    explicit BufferManager(size_t capacity = 2);
    ~BufferManager();

    /**
    * Submit a next of the file given by the descriptor at the given offset. A previously used buffer can be given back,
    * there is no guarantee, that the data will be next into the buffer.
    * @param fd
    * @param buffer
    * @param offset
    */
    void read(int fd, Buffer *buffer, size_t &offset);

    /**
     * Wait for the completion of the next previously issued for the file fd.
     * @param fd
     * @return The buffer if the next is completed, or nullptr if the file is over
     */
    Buffer *wait(int fd);

    /**
     * Get an aligned buffer from the manager.
     * @param buffer
     */
    Buffer *take();

    /**
     * Return a buffer to the manager for it to reuse.
     * @param buffer
     */
    void give(Buffer *buffer);

    // for stats only
    int max_fd = -1;
    unsigned long waited_total = 0;
};