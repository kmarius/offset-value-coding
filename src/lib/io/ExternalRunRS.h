#pragma once

#include <string>
#include <fcntl.h>
#include <stdexcept>
#include <cstring>
#include "Buffer.h"
#include "BufferManager.h"
#include "lib/Row.h"

namespace ovc::io {
    class ExternalRunRS {

        std::string path_;
        int fd;
        uint8_t buffer[BUFFER_SIZE];
        size_t rows;
        size_t cur;

    public:
        explicit ExternalRunRS(const std::string &path);

        ~ExternalRunRS();

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
        * Flush all buffers and close the file. This function is automatically called on destruction.
        */
        void finalize();

        /**
         * Finalizes the run and removes the file.
         */
        void remove();
    };
}