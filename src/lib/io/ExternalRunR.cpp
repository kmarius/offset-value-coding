#include "ExternalRunR.h"
#include "lib/defs.h"
#include "lib/log.h"

#define RUN_EMPTY ((size_t) -1)

ExternalRunR::ExternalRunR() : fd(-1) {

}

ExternalRunR::ExternalRunR(const std::string &path,
                           BufferManager &buffer_manager) : path_(path), offset(0),
                                                            buffer(nullptr),
                                                            buffer_manager(&buffer_manager), rows(0), cur(0) {
    fd = open(path.c_str(), O_RDONLY
                            #ifdef USE_O_DIRECT
                            | O_DIRECT
#endif
    );

#ifdef USE_O_DIRECT
    if (fd < 0 && errno == EINVAL) {
        /* O_DIRECT is not supported e.g. in tempfs */
        log_info("open failed with EINVAL, retrying without O_DIRECT");
        fd = open(path.c_str(), O_RDONLY);
    }
#endif

    if (fd < 0) {
        throw std::runtime_error(std::string("open: ") + strerror(errno));
    }
    buffer_manager.read(fd, nullptr, offset);
}

ExternalRunR::~ExternalRunR() {
    finalize();
}

const std::string &ExternalRunR::path() const {
    return path_;
}

Row *ExternalRunR::read() {
    assert(fd > 0);
    if (rows == RUN_EMPTY) {
        return nullptr;
    }
    if (buffer == nullptr) {
        buffer = buffer_manager->wait(fd);

        if (buffer == nullptr) {
            rows = RUN_EMPTY;
            return nullptr;
        }

        // first eight bytes indicate how many rows there are in the current buffer
        rows = *(size_t *) buffer;
        cur = 0;

        // we never save isEmpty pages
        if (unlikely(rows == 0)) {
            log_error("Empty page in %s", path().c_str());
        }
        assert(rows > 0);
    }

    Row *res = &((Row *) ((uint8_t *) buffer->data + sizeof(rows)))[cur];
    cur++;
    rows--;

    // TODO: why do we need buffer for the last two rows? Fix this!
    // technically, one global buffer for the last but one row would be enough, I think
    if (rows == 1) {
        buf[1] = *res;
        res = &buf[1];
    }

    if (rows == 0) {
        buf[0] = *res;
        res = &buf[0];

        buffer_manager->read(fd, buffer, offset);
        buffer = nullptr;
    }

    return res;
}

void ExternalRunR::remove() {
    finalize();
    std::remove(path().c_str());
}

void ExternalRunR::finalize() {
    if (fd > 0) {
        log_trace("finalizing %s", path_.c_str());
        // TODO: if we are currently loading, we might have to wait for the completion before closing
        if (buffer == nullptr && rows != RUN_EMPTY) {
            buffer = buffer_manager->wait(fd);
        }
        if (buffer != nullptr) {
            buffer_manager->give(buffer);
        }
        close(fd);
        fd = -1;
        rows = RUN_EMPTY;
    }
}