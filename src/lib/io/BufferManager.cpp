#include "BufferManager.h"
#include "lib/log.h"

#include <cstring>
#include <memory>

namespace ovc::io {

    BufferManager::BufferManager(size_t capacity) : ring(), loading(), completed(), capacity(capacity) {
        if (io_uring_queue_init(512, &ring, 0) < 0) {
            throw std::runtime_error("error initializing io_uring");
        }

        size_t size = capacity * BUFFER_SIZE;
        size_t space = size + BUFFER_ALIGNMENT;
        buffers_raw = new uint8_t[space];
        void *p = buffers_raw;
        if (std::align(BUFFER_ALIGNMENT, size, p, space) == nullptr) {
            throw std::runtime_error("failed to align buffer memory");
        }
        buffers_aligned = (Buffer *) p;

        for (size_t i = 0; i < capacity; i++) {
            free[capacity - i - 1] = i;
        }
        free_ptr = capacity;
    }

    BufferManager::~BufferManager() {
        // TODO: do we need to wait for all reads? What happens if the files are closed before reads finish?
        io_uring_queue_exit(&ring);

        for (auto &a: loading) {
            give(a.second);
        }

        // all buffer should have been returned
        assert(free_ptr == capacity);

        delete[] buffers_raw;
    }

    void BufferManager::read(int fd, Buffer *buffer, size_t &offset) {
        assert(fd < MAX_DESCRIPTORS);
        assert(offset % BUFFER_ALIGNMENT == 0);

        if (fd > max_fd) {
            max_fd = fd;
        }

        if (loading.find(fd) != loading.end()) {
            // TODO: this means we are already loading in the file, preload something else with the new buffer
            return;
        }

        if (buffer == nullptr) {
            assert(free_ptr > 0);
            buffer = &buffers_aligned[free[--free_ptr]];
        }

        assert(is_aligned(buffer->data, BUFFER_ALIGNMENT));

        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        io_uring_prep_read(sqe, fd, buffer, BUFFER_SIZE, offset);
        io_uring_sqe_set_data64(sqe, (size_t) fd);
        io_uring_submit(&ring);

        loading[fd] = buffer;
        completed[fd] = false;
        offset += BUFFER_SIZE;
    }

    Buffer *BufferManager::wait(int fd) {
        auto it = loading.find(fd);
        assert(it != loading.end());

        while (!completed[fd]) {
            io_uring_submit_and_wait(&ring, 1);
            io_uring_cqe *cqe;
            unsigned head;
            int processed = 0;
            io_uring_for_each_cqe(&ring, head, cqe) {
                if (cqe->res < 0) {
                    log_error("Error: %s (%d)", strerror(-cqe->res), -cqe->res);
                }
                assert(cqe->res >= 0);
                auto read_fd = (int) io_uring_cqe_get_data64(cqe);
                if (cqe->res == 0) {
                    // nothing left in the file TODO: reuse it
                    auto it = loading.find(read_fd);
                    assert(it != loading.end());
                    give(it->second);
                    it->second = nullptr;
                }
                completed[read_fd] = true;
                processed++;
            }
            io_uring_cq_advance(&ring, processed);
        }

        auto buffer = it->second;
        loading.erase(it);
        completed[fd] = false;

        return buffer;
    }

    void BufferManager::give(Buffer *buffer) {
        if (buffer != nullptr) {
            assert(buffer >= buffers_aligned);
            assert(buffer - buffers_aligned < capacity);
            free[free_ptr++] = buffer - buffers_aligned;
        }
        assert(free_ptr <= capacity);
    }

    Buffer *BufferManager::take() {
        assert(free_ptr > 0);
        return &buffers_aligned[free[--free_ptr]];
    }
}