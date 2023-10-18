#include "ExternalRunR.h"
#include "lib/defs.h"
#include "lib/log.h"

#define RUN_EMPTY ((size_t) -1)

namespace ovc::io {

    ExternalRunR::ExternalRunR() : fd(-1) {

    }

    ExternalRunR::ExternalRunR(std::string path,
                               BufferManager &buffer_manager, bool no_throw) : path_(path), offset(0),
                                                                               buffer(nullptr),
                                                                               buffer_manager(&buffer_manager),
                                                                               rows(0),
                                                                               cur(0),
                                                                               prev(nullptr) {
        log_trace("opening %s", path.c_str());
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
            if (no_throw) {
                rows = RUN_EMPTY;
            } else {
                throw std::runtime_error(std::string("open: ") + strerror(errno));
            }
        } else {
            buffer_manager.read(fd, nullptr, offset);
        }
    }

    bool ExternalRunR::definitelyEmpty() const {
        return rows == RUN_EMPTY;
    }

    ExternalRunR::~ExternalRunR() {
        finalize();
    }

    const std::string &ExternalRunR::path() const {
        return path_;
    }

    Row *ExternalRunR::read() {
        if (rows == RUN_EMPTY) {
            return nullptr;
        }
        if (buffer == nullptr) {
            if (prev != nullptr) {
                buffer_manager->give(prev);
                prev = nullptr;
            }
            buffer = buffer_manager->wait(fd);

            if (buffer == nullptr) {
                rows = RUN_EMPTY;
                return nullptr;
            }

            // first eight bytes indicate how many rows there are in the current buffer
            rows = *(size_t *) buffer;
            cur = 0;

            // we currently never save empty pages
            if (unlikely(rows == 0)) {
                log_error("Empty page in %s", path().c_str());
                rows = RUN_EMPTY;
                return nullptr;
            }
            assert(rows > 0);
        }

        Row *res = &((Row *) ((uint8_t *) buffer->data + sizeof(rows)))[cur];
        cur++;

        if (rows - cur == 0) {
            buffer_manager->read(fd, nullptr, offset);
            prev = buffer;
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
            if (prev != nullptr) {
                buffer_manager->give(prev);
            }
            close(fd);
            fd = -1;
            rows = RUN_EMPTY;
        }
    }
}