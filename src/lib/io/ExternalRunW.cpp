#include "ExternalRunW.h"
#include "lib/defs.h"
#include "lib/log.h"
#include "lib/utils.h"

namespace ovc::io {

    ExternalRunW::ExternalRunW() : fd(-1) {

    }

    ExternalRunW::ExternalRunW(const std::string &path, BufferManager &buffer_manager) :
            current(0), rows(0), rows_total(0), offset(0), path_(path), buffer_manager(&buffer_manager) {

        fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC
                                #ifdef USE_O_DIRECT
                                | O_DIRECT
#endif
                , 0644);

#ifdef USE_O_DIRECT
        if (fd < 0 && errno == EINVAL) {
            /* O_DIRECT is not supported in e.g. tempfs */
            log_info("open failed with EINVAL, retrying without O_DIRECT");
            fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        }
#endif

        if (fd < 0) {
            throw std::runtime_error(std::string("open: ") + strerror(errno));
        }

        if (io_uring_queue_init(2, &ring, 0) < 0) {
            throw std::runtime_error("error initializing io_uring");
        }

        buffers[0] = buffer_manager.take();
        buffers[1] = buffer_manager.take();

        assert(is_aligned(buffers[0]->data, BUFFER_ALIGNMENT));
        assert(is_aligned(buffers[1]->data, BUFFER_ALIGNMENT));

        // first bytes area the "header"
        sizes[0] = sizeof(size_t);
        sizes[1] = sizeof(size_t);
        ready[0] = true;
        ready[1] = true;
    }

    ExternalRunW::~ExternalRunW() {
        finalize();
    }

    void ExternalRunW::add(Row &row) {
        assert(fd > 0);
        write_to_buffer(&row, sizeof(Row));
        rows++;
        rows_total++;
    }

    size_t ExternalRunW::size() const {
        return rows_total;
    }

    void ExternalRunW::write_to_buffer(void *data, size_t size) {
        assert(size <= BUFFER_SIZE);
        if (size > BUFFER_SIZE - sizes[current]) {
            flush(current);
            current ^= 1;
            wait_for_write_completion(current);
            rows = 0;
        }
        memcpy(buffers[current]->data + sizes[current], data, size);
        sizes[current] += size;
    }

    void ExternalRunW::submit_write(int index) {
        assert(is_aligned(buffers[index]->data, BUFFER_ALIGNMENT));
        memcpy(buffers[index]->data, &rows, sizeof(rows));
        io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        io_uring_prep_write(sqe, fd, buffers[index]->data, BUFFER_SIZE, offset);
        io_uring_sqe_set_data64(sqe, index);
        io_uring_submit(&ring);
        ready[index] = false;
        offset += BUFFER_SIZE;
    }

    void ExternalRunW::wait_for_write_completion(int index) {
        while (!ready[index]) {
            io_uring_submit_and_wait(&ring, 1);
            io_uring_cqe *cqe;
            unsigned head;
            int processed = 0;
            io_uring_for_each_cqe(&ring, head, cqe) {
                auto ind = io_uring_cqe_get_data64(cqe);
                if (cqe->res != BUFFER_SIZE) {
                    log_error("add failed for %s: %s", path_.c_str(), strerror(-cqe->res));
                    assert(cqe->res > 0);
                }
                ready[ind] = true;
                sizes[ind] = sizeof(size_t);
                processed++;
            }
            io_uring_cq_advance(&ring, processed);
        }
    }

    void ExternalRunW::flush(int index) {
        submit_write(index);
#ifndef SYNC_IO
#else
        wait_for_write_completion(index);
#endif
    }

    const std::string &ExternalRunW::path() const {
        return path_;
    }

    void ExternalRunW::finalize() {
        if (fd > 0) {
            log_trace("finalizing %s (fd=%d)", path_.c_str(), fd);
            if (rows > 0) {
                flush(current);
            }
            wait_for_write_completion(0);
            wait_for_write_completion(1);

            io_uring_queue_exit(&ring);
            close(fd);

            buffer_manager->give(buffers[0]);
            buffer_manager->give(buffers[1]);

            fd = -1;
        }
    }
}