#include <cstdint>
#include "Row.h"
#include "log.h"

std::ostream &operator<<(std::ostream &stream, const Row &row) {
    stream << "[" << row.key << ": ";
    for (int i = 0; i < ROW_ARITY; ++i) {
        if (i > 0) {
            stream << ", ";
        }
        stream << row.columns[i];
    }
    stream << "]";
    return stream;
}

bool Row::equals(const Row &row) const {
//#pragma unroll
    for (int i = 0; i < ROW_ARITY; i++) {
        if (columns[i] != row.columns[i]) {
            return false;
        }
    }
    return true;
}

bool Row::less(const Row &row) const {
#pragma unroll
    for (int i = 0; i < ROW_ARITY; i++) {
        if (columns[i] != row.columns[i]) {
            return columns[i] < row.columns[i];
        }
    }
    return false;
}

bool Row::less(const Row &row, OVC &ovc) {
    int offset = 0;
    for (; offset < ROW_ARITY && columns[offset] == row.columns[offset]; offset++) {}

    if (offset == ROW_ARITY) {
        // rows are equal
        ovc = 0;
        return false;
    }

    assert(offset < ROW_ARITY);

    if (columns[offset] < row.columns[offset]) {
        // we are smaller
        ovc = DOMAIN * (ROW_ARITY - offset) + row.columns[offset];
        return true;
    } else {
        // we are larger
        ovc = DOMAIN * (ROW_ARITY - offset) + columns[offset];
        return false;
    }
}

const char *Row::c_str() const {
    static char buf[128];
    int pos = sprintf(buf, "[%lu: ", key);
    for (int i = 0; i < ROW_ARITY; i++) {
        pos += sprintf(buf + pos, "%lu", columns[i]);
        if (i < ROW_ARITY - 1) {
            pos += sprintf(buf + pos, ", ");
        }
    }
    sprintf(buf + pos, "]");
    return buf;
}

/*
 * https://en.m.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
 */
static inline uint64_t hashl(const char *buf, size_t l) {
    uint64_t h = 0xcbf29ce484222325;
    for (size_t i = 0; i < l; i++) {
        h = (h ^ buf[i]) * 0x00000100000001b3;
    }
    return h;
}

unsigned long Row::setHash() {

    /*
    unsigned long h = 0xcbf29ce484222325;
    for (unsigned long column: columns) {
        h ^= hashl((char *) &column, sizeof(column)) + (h << 6) + (h >> 2);
        //h ^= hashl((char *) &column, sizeof(column));
    }
    key = h;
     */

    key = hashl((char *) columns, sizeof(columns));

    return key;
}
