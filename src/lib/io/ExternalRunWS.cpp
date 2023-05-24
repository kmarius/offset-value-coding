#include <fcntl.h>
#include <cstring>
#include <unistd.h>

#include "ExternalRunWS.h"
#include "Buffer.h"
#include "lib/log.h"

void ExternalRunWS::add(Row &row) {
    assert(sizeof(row) < BUFFER_SIZE);
    if (offset + sizeof(row) > BUFFER_SIZE) {
        flush();
    }
    memcpy(buffer + offset, &row, sizeof(row));
    offset += sizeof(row);
    rows++;
}

void ExternalRunWS::finalize() {
    if (fd > 0) {
        log_trace("finalizing %s", path_.c_str());
        flush();
        close(fd);
        fd = -1;
    }
}

const std::string &ExternalRunWS::path() const {
    return path_;
}

ExternalRunWS::ExternalRunWS(const std::string &path) : path_(path), rows(0), offset(sizeof(size_t)) {
    fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::runtime_error(std::string("open: ") + strerror(errno));
    }
}

void ExternalRunWS::flush() {
    if (rows > 0) {
        *((size_t *) &buffer) = rows;
        ::write(fd, buffer, BUFFER_SIZE);
        rows = 0;
        offset = sizeof(size_t);
    }
}

ExternalRunWS::~ExternalRunWS() {
    finalize();
}