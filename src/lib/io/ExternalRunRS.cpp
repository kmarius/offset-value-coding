#include "ExternalRunRS.h"
#include "lib/log.h"

#define RUN_EMPTY ((size_t) -1)

namespace ovc::io {

    ExternalRunRS::ExternalRunRS(const std::string &path) : path_(path), rows(0), cur(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error(std::string("open: ") + strerror(errno));
        }
    }

    ExternalRunRS::~ExternalRunRS() {
        finalize();
    }

    const std::string &ExternalRunRS::path() const {
        return path_;
    }

    Row *ExternalRunRS::read() {
        if (rows == RUN_EMPTY) {
            return nullptr;
        }
        if (rows == 0) {
            ssize_t ret = ::read(fd, buffer, BUFFER_SIZE);
            if (ret == 0) {
                rows = RUN_EMPTY;
                return nullptr;
            }
            if (ret < 0) {
                log_error("next: %s", strerror(errno));
            }
            rows = *(size_t *) buffer;
            cur = 0;
        }

        assert(rows > 0);
        Row *res = &((Row *) (buffer + sizeof(size_t)))[cur];
        cur++;
        rows--;
        return res;
    }

    void ExternalRunRS::finalize() {
        if (fd > 0) {
            log_trace("finalizing %s", path_.c_str());
            close(fd);
            fd = -1;
        }
    }

    void ExternalRunRS::remove() {
        finalize();
        ::remove(path_.c_str());
    }
}