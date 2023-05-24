#pragma once

#include <string>
#include <cstdint>
#include "lib/Row.h"
#include "Buffer.h"

class ExternalRunWS {
private:
    uint8_t buffer[BUFFER_SIZE];

    std::string path_;
    int fd;

    size_t offset;

    // Number of rows in the current buffer
    size_t rows;

    void flush();
public:
    explicit ExternalRunWS(const std::string &path);

    ~ExternalRunWS();

    /**
     * Write a row to this run.
     * @param row
     */
    void add(Row &row);

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