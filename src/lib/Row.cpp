#include <cstdint>

#include "Row.h"
#include "log.h"

namespace ovc {

    unsigned long row_equality_column_comparisons = 0;
    unsigned long row_num_calls_to_hash = 0;
    unsigned long row_num_calls_to_equal = 0;

    unsigned long row_equal_prefix = 0;
    unsigned long row_less_prefix = 0;

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
        for (int i = 0; i < ROW_ARITY; i++) {
            if (columns[i] != row.columns[i]) {
                return false;
            }
        }
        return true;
    }


    const char *Row::c_str() const {
        static char buf[128];
        int pos = sprintf(buf, "[%d@%d:%lu: ", getOVC().getValue(), ROW_ARITY - getOVC().getOffset(), tid);
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

    unsigned long Row::setHash(int hash_columns) {

        /*
        unsigned long h = 0xcbf29ce484222325;
        for (unsigned long column: columns) {
            h ^= hashl((char *) &column, sizeof(column)) + (h << 6) + (h >> 2);
            //h ^= hashl((char *) &column, sizeof(column));
        }
        key = h;
         */

        key = hashl((char *) columns, hash_columns * sizeof columns[0]);

        return key;
    }
}