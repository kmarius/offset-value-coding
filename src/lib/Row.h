#pragma once

#include <ostream>
#include <cassert>
#include <vector>
#include <cstdint>
#include <cstring>

#include "defs.h"
#include "log.h"

#define ROW_ARITY 32

#define BITMASK(bits) ((1ul << (bits)) - 1)

// bits used for the offset, 7 mean we can work with 128 columns
#define ROW_OFFSET_BITS 7

#define ROW_OFFSET_MASK (BITMASK(ROW_OFFSET_BITS) << ROW_VALUE_BITS)

#define ROW_VALUE_BITS (8 * sizeof(ovc_type_t) - ROW_OFFSET_BITS)

#define ROW_VALUE_MASK (BITMASK(ROW_VALUE_BITS))

#define MAKE_OVC(arity, offset, value) ((((arity) - (offset)) << ROW_VALUE_BITS) & ROW_OFFSET_MASK | ((value) & ROW_VALUE_MASK))

#define OVC_SET_OFFSET(ovc, offset, arity) (((ovc) & ~ROW_OFFSET_MASK) | (((arity)-(offset)) << ROW_VALUE_BITS))
#define OVC_GET_OFFSET(ovc, arity) ((arity) - (((ovc) & ROW_OFFSET_MASK) >> ROW_VALUE_BITS))
#define OVC_GET_VALUE(ovc) ((ovc) & ROW_VALUE_MASK)

#define OVC_FMT(ovc) OVC_GET_VALUE(ovc), OVC_GET_OFFSET(ovc, ROW_ARITY)

namespace ovc {

    typedef uint32_t ovc_type_t;

    typedef struct Row {
        OVC key;
        unsigned long tid;
        unsigned long columns[ROW_ARITY];

        inline unsigned long getOffset(unsigned long arity = ROW_ARITY) const {
            return arity - ((key & ROW_OFFSET_MASK) >> ROW_VALUE_BITS);
        }

        inline unsigned long getValue(unsigned long arity = ROW_ARITY) const {
            return key & ROW_VALUE_MASK;
        }

        inline void setOVC(const Row &base, int prefix = ROW_ARITY, struct iterator_stats *stats = nullptr) {
            OVC ovc = calcOVC(base, prefix, stats);
            key = ovc;
        }

        inline void setNewOVC(int arity, int offset, int idx) {
            key = MAKE_OVC(arity, offset, columns[idx]);
        }

        inline void setOVCInitial(int prefix = ROW_ARITY, struct iterator_stats *stats = nullptr) {
            key = MAKE_OVC(prefix, 0, columns[0]);
        }

        long calcOVC(const Row &base, int prefix = ROW_ARITY, struct iterator_stats *stats = nullptr) const {
            int offset = 0;
            long cmp;
            for (; offset < prefix; offset++) {
                cmp = columns[offset] - base.columns[offset];
                if (stats) {
                    stats->column_comparisons++;
                }
                if (cmp) {
                    break;
                }
            }
            if (offset >= prefix) {
                return 0;
            }
            if (cmp < 0) {
                log_error("calcOVC: base is larger than row");
                return -1;
            }
            return MAKE_OVC(ROW_ARITY, offset, columns[offset]);
        }

        bool equals(const Row &row) {
            return memcmp(columns, row.columns, sizeof columns) == 0;
        }

        unsigned long setHash(int hash_columns = ROW_ARITY);

        unsigned long calcHash(int hash_columns = ROW_ARITY);

        /**
         * Get a string representation (in a statically allocated buffer).
         * @return The string.
         */
        const char *c_str(char *buf = nullptr) const {
            static char sbuf[256];
            if (buf == nullptr) {
                buf = sbuf;
            }
            int pos = sprintf(buf, "[%lu@%lu:%lu: ", getValue(), getOffset(), tid);
            for (int i = 0; i < ROW_ARITY; i++) {
                pos += sprintf(buf + pos, "%lu", columns[i]);
                if (i < ROW_ARITY - 1) {
                    pos += sprintf(buf + pos, ", ");
                }
            }
            sprintf(buf + pos, "]");
            return buf;
        }

        friend std::ostream &operator<<(std::ostream &stream, const Row &row);
    } Row;
}