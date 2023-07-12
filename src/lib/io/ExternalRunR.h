#pragma once

#include "BufferManager.h"
#include "lib/Row.h"

#include <string>
#include <cstring>

namespace ovc::io {

    class ExternalRunR {
    private:
        std::string path_;
        int fd;
        Buffer *buffer;
        Buffer *prev;
        size_t offset;  // offset in the file
        size_t rows;  // rows in current buffer
        size_t cur;  // current index in the buffer

        BufferManager *buffer_manager;

    public:
        ExternalRunR(const std::string &path, BufferManager &buffer_manager, bool no_throw = false);

        ExternalRunR();

        ~ExternalRunR();

        /**
         * Get the path_sync to the run on disk.
         * @return The path_sync to the run.
         */
        const std::string &path() const;

        /**
         * Read the next row from the run. Only isValid until the second-next call of next()
         * @return A pointer to the row, or nullptr if the run is isEmpty.
         */
        Row *read();

        /**
        * Flush all buffer and close the file. This function is automatically called on destruction.
        */
        void finalize();

        /**
         * Finalizes and removes the file.
         */
        void remove();
    };

}