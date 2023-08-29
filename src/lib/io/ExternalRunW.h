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

namespace ovc::io {

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

        // did we spill to disk?
        bool did_spill;

        // lazy open the file when we write the first buffer page
        bool lazy_open;

        void _open();

        void flush(int index);

        void submit_write(int index);

        void wait_for_write_completion(int index);

        void write_to_buffer(void *data, size_t size);

    public:
        explicit ExternalRunW(std::string path, BufferManager &buffer_manager, bool lazy_open = false);

        explicit ExternalRunW();

        ~ExternalRunW();

        /**
         * Add a row to this run.
         * @param row
         */
        void add(Row &row);

        Row *back();

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

        void discard();

        bool didSpill() const {
            return did_spill;
        }

        Row *begin_page() {
            return reinterpret_cast<Row *>(buffers[current]->data + sizeof(size_t));
        }

        const Row *end_page() {
            return reinterpret_cast<Row *>(buffers[current]->data + sizes[current]);
        }

        // assume keys are hash values and compare fully using by prefix
        template<typename Equal>
        Row *probe_page(const Row *row, Equal &eq) {
            Row *it = begin_page();
            const Row *end = end_page();
            for (; it < end; it++) {
                if (row->key == it->key && eq(*row, *it)) {
                    return it;
                }
            }
            return nullptr;
        };
    };
}