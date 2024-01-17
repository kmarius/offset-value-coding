#pragma once

#include <ostream>
#include <cassert>
#include <vector>
#include <cstdint>
#include <cstring>

#include "defs.h"
#include "log.h"

#define ROW_ARITY 8
#define DOMAIN 100

#define BITMASK(bits) ((1ul << (bits)) - 1)

// bits used for the offset, 7 mean we can work with 128 columns
#define ROW_OFFSET_BITS 7

#define ROW_OFFSET_MASK (BITMASK(ROW_OFFSET_BITS) << ROW_VALUE_BITS)

#define ROW_VALUE_BITS (8 * sizeof(ovc_type_t) - ROW_OFFSET_BITS)

#define ROW_VALUE_MASK (BITMASK(ROW_VALUE_BITS))

#define MAKE_OVC(arity, offset, value) (((arity - offset) << ROW_VALUE_BITS) & ROW_OFFSET_MASK | (value & ROW_VALUE_MASK))

namespace ovc {

    typedef uint32_t ovc_type_t;


// statistics: number of times hash was called
    extern unsigned long row_num_calls_to_hash;

// statistics: number of times == was called
    extern unsigned long row_num_calls_to_equal;

    struct OVC_s {
        ovc_type_t ovc;

        OVC_s(ovc_type_t ovc) : ovc(ovc) {};

        unsigned getOffset() const {
            return ROW_ARITY - ((ovc & ROW_OFFSET_MASK) >> ROW_VALUE_BITS);
        };

        int getValue() const {
            return ovc & ROW_VALUE_MASK;
        };
    };

    typedef struct Row {
        OVC key;
        unsigned long tid;
        unsigned long columns[ROW_ARITY];

        OVC_s getOVC() const {
            return key;
        }

        inline void setOVC(const Row &base, int prefix = ROW_ARITY, struct iterator_stats *stats = nullptr) {
            OVC ovc = calcOVC(base, prefix, stats);
            key = ovc;
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

        unsigned long setHash(int hash_columns = ROW_ARITY);

        unsigned long calcHash(int hash_columns = ROW_ARITY);

        bool equals(const Row &row) const {
            return cmp(row, nullptr, 0) == 0;
        };

        inline long cmp_impl(const Row &row, OVC *ovc, unsigned int offset, unsigned cmp_columns,
                             struct iterator_stats *stats) const {
            long cmp;

            for (; offset < cmp_columns; offset++) {
                cmp = columns[offset] - row.columns[offset];
                if (stats) {
                    stats->column_comparisons++;
                }
                if (cmp) {
                    break;
                }
            }

            if (offset >= cmp_columns) {
                // rows are equal
                if (ovc) {
                    *ovc = 0;
                }
                return 0;
            }
            if (ovc) {
                if (cmp < 0) {
                    // we are smaller
                    *ovc = MAKE_OVC(ROW_ARITY, offset, row.columns[offset]);
                } else {
                    // we are larger
                    *ovc = MAKE_OVC(ROW_ARITY, offset, columns[offset]);
                }
            }
            return cmp;
        }


        /**
         * cmp semantics, writes the OVC of the loser w.r.t the winner to the provided address
         * @param row
         * @param ovc
         * @return
         */
        inline long
        cmp(const Row &row, OVC *ovc = nullptr, unsigned int offset = 0, struct iterator_stats *stats = nullptr) const {
            return cmp_impl(row, ovc, offset, ROW_ARITY, stats);
        }

        /**
         * Get a string representation (in a statically allocated buffer).
         * @return The string.
         */
        const char *c_str(char *buf = nullptr) const {
            static char sbuf[128];
            if (buf == nullptr) {
                buf = sbuf;
            }
            int pos = sprintf(buf, "[%d@%d:%lu: ", getOVC().getValue(), getOVC().getOffset(), tid);
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

    struct CmpColumnListNoOVC {
        uint8_t columns[ROW_ARITY];
        int length;
        struct iterator_stats *stats;
    private:
        CmpColumnListNoOVC() : columns(), length(0), stats(nullptr) {};

    public:
        CmpColumnListNoOVC addStats(struct iterator_stats *stats) const {
            CmpColumnListNoOVC cmp;
            cmp.stats = stats;
            cmp.length = length;
            mempcpy(cmp.columns, columns, length * sizeof *columns);
            return cmp;
        }

        explicit CmpColumnListNoOVC(std::initializer_list<uint8_t> columns, iterator_stats *stats = nullptr)
                : length(columns.size()), stats(stats) {
            assert(columns.size() <= ROW_ARITY);
            int i = 0;
            for (auto col: columns) {
                this->columns[i++] = col;
            }
        };

        explicit CmpColumnListNoOVC(uint8_t *cols, int n, iterator_stats *stats = nullptr)
                : length(n), stats(stats) {
            assert(n <= ROW_ARITY);
            for (int i = 0; i < n; i++) {
                columns[i] = cols[i];
            }
        };

        long operator()(const Row &lhs, const Row &rhs) const {
            for (int i = 0; i < length; i++) {
                if (stats) {
                    stats->column_comparisons++;
                }
                uint8_t ind = columns[i];
                long cmp = (long) lhs.columns[ind] - (long) rhs.columns[ind];
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        long raw(const Row &lhs, const Row &rhs) const {
            for (int i = 0; i < length; i++) {
                uint8_t ind = columns[i];
                long cmp = (long) lhs.columns[ind] - (long) rhs.columns[ind];
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        inline unsigned long makeOVC(long arity, long offset, const Row *row) const {
            log_error("makeOVC called on CmpPrefixNoOVC");
            return 0;
        }
    };

    struct CmpPrefixNoOVC : public CmpColumnListNoOVC {
    public:
        CmpPrefixNoOVC() = delete;

        explicit CmpPrefixNoOVC(int prefix, iterator_stats *stats = nullptr)
                : CmpColumnListNoOVC(nullptr, 0, stats) {
            length = prefix;
            for (int i = 0; i < prefix; i++) {
                columns[i] = i;
            }
        };
    };

    struct CmpNoOVC : public CmpPrefixNoOVC {
    public:
        explicit CmpNoOVC(iterator_stats *stats = nullptr) : CmpPrefixNoOVC(ROW_ARITY, stats) {}
    };

    struct CmpColumnListOVC {
        uint8_t columns[ROW_ARITY];
        int length;
        struct iterator_stats *stats;
    private:
        CmpColumnListOVC() : columns(), length(0), stats(nullptr) {};

    public:
        CmpColumnListOVC addStats(struct iterator_stats *stats) const {
            CmpColumnListOVC cmp;
            cmp.stats = stats;
            cmp.length = length;
            mempcpy(cmp.columns, columns, length * sizeof *columns);
            return cmp;
        }

        inline unsigned long makeOVC(long arity, long offset, const Row *row) const {
            return MAKE_OVC(arity, offset, row->columns[columns[offset]]);
        }

        explicit CmpColumnListOVC(std::initializer_list<uint8_t> columns, iterator_stats *stats = nullptr)
                : length(columns.size()), stats(stats) {
            assert(columns.size() <= ROW_ARITY);
            int i = 0;
            for (auto col: columns) {
                this->columns[i++] = col;
            }
        };

        long operator()(Row &lhs, Row &rhs) const {
            long cmp = lhs.key - rhs.key;
            if (cmp == 0) {
                if (lhs.key == 0) {
                    // equality, no need to go on
                    return 0;
                }

                // if i > prefix, rows are equal
                int i = lhs.getOVC().getOffset() + 1;
                for (; i < length; i++) {
                    cmp = lhs.columns[columns[i]] - rhs.columns[columns[i]];
                    if (stats) {
                        stats->column_comparisons++;
                    }
                    if (cmp) {
                        break;
                    }
                }

                if (i >= length) {
                    // rows are equal
                    rhs.key = 0;
                    return 0;
                }

                if (cmp <= 0) {
                    rhs.key = MAKE_OVC(ROW_ARITY, i, rhs.columns[columns[i]]);
                }
                if (cmp > 0) {
                    lhs.key = MAKE_OVC(ROW_ARITY, i, lhs.columns[columns[i]]);
                }
            }
            return cmp;
        }

        long raw(Row &lhs, Row &rhs) const {
            long cmp = lhs.key - rhs.key;
            if (cmp == 0) {
                if (lhs.key == 0) {
                    // equality, no need to go on
                    return 0;
                }

                // if offset > prefix, rows are equal
                int offset = lhs.getOVC().getOffset() + 1;

                for (; offset < length; offset++) {
                    cmp = lhs.columns[offset] - rhs.columns[offset];
                    if (cmp) {
                        break;
                    }
                }

                if (offset >= length) {
                    // rows are equal
                    rhs.key = 0;
                    return 0;
                }

                if (cmp <= 0) {
                    rhs.key = MAKE_OVC(ROW_ARITY, offset, rhs.columns[offset]);
                }
                if (cmp > 0) {
                    lhs.key = MAKE_OVC(ROW_ARITY, offset, lhs.columns[offset]);
                }
            }
            return cmp;
        }
    };


    // Compares two rows both having offset value codes relative to the same base row.
    // Sets the offset value code of the loser (bigger row) relative to the winner (smaller row).
    // If the rows are equal, the offset value code of the right row is set to 0.
    struct RowCmpPrefixOVC {
        const int prefix;
        struct iterator_stats *stats;
    public:
        RowCmpPrefixOVC() = delete;

        explicit RowCmpPrefixOVC(int prefix, iterator_stats *stats = nullptr) : prefix(prefix), stats(stats) {};

        inline unsigned long makeOVC(long arity, long offset, const Row *row) const {
            return MAKE_OVC(arity, offset, row->columns[offset]);
        }

        long operator()(Row &lhs, Row &rhs) const {
            long cmp = lhs.key - rhs.key;
            if (cmp == 0) {
                if (lhs.key == 0) {
                    // equality, no need to go on
                    return 0;
                }

                // if offset > prefix, rows are equal
                int offset = lhs.getOVC().getOffset() + 1;

                OVC ovc;
                cmp = lhs.cmp_impl(rhs, &ovc, offset, prefix, stats);

                if (cmp <= 0) {
                    rhs.key = ovc;
                }
                if (cmp > 0) {
                    lhs.key = ovc;
                }
            }
            return cmp;
        }

        long raw(Row &lhs, Row &rhs) const {
            return lhs.cmp_impl(rhs, nullptr, 0, prefix, nullptr);
        }
    };

    struct RowCmpOVC : public RowCmpPrefixOVC {
    public:
        explicit RowCmpOVC(iterator_stats *stats = nullptr) : RowCmpPrefixOVC(ROW_ARITY, stats) {}
    };

    struct RowEqualPrefixNoOVC {
        const int prefix;
        struct iterator_stats *stats;
    public:
        RowEqualPrefixNoOVC() = delete;

        explicit RowEqualPrefixNoOVC(const int prefix, struct iterator_stats *stats = nullptr)
                : prefix(prefix), stats(stats) {}

        int operator()(const Row &lhs, const Row &rhs) const {
            for (int i = 0; i < prefix; i++) {
                if (stats) {
                    stats->column_comparisons++;
                }
                if (lhs.columns[i] != rhs.columns[i]) {
                    return false;
                }
            }
            return true;
        }
    };

    struct EqColumnListNoOVC {
        uint8_t columns[ROW_ARITY];
        int length;
        struct iterator_stats *stats;
    public:
        explicit EqColumnListNoOVC(std::initializer_list<uint8_t> columns, iterator_stats *stats = nullptr)
                : length(columns.size()), stats(stats) {
            assert(columns.size() <= ROW_ARITY);
            int i = 0;
            for (auto col: columns) {
                this->columns[i++] = col;
            }
        };

        long operator()(Row &lhs, Row &rhs) const {
            for (int i = 0; i < length; i++) {
                if (stats) {
                    stats->column_comparisons++;
                }
                if (lhs.columns[columns[i]] != rhs.columns[columns[i]]) {
                    return false;
                };
            }
            return true;
        }

        long raw(Row &lhs, Row &rhs) const {
            for (int i = 0; i < length; i++) {
                if (lhs.columns[columns[i]] != rhs.columns[columns[i]]) {
                    return false;
                };
            }
            return true;
        }
    };

    struct RowEqual {
        struct iterator_stats *stats;
    public:
        RowEqual() = delete;

        explicit RowEqual(struct iterator_stats *stats = nullptr) : stats(stats) {}

        int operator()(const Row &lhs, const Row &rhs) const {
            for (int i = 0; i < ROW_ARITY; i++) {
                if (stats) {
                    stats->column_comparisons++;
                }
                if (lhs.columns[i] != rhs.columns[i]) {
                    return false;
                }
            }
            return true;
        }
    };
}

namespace std {
    template<>
    struct hash<ovc::Row> {
        std::size_t operator()(const ovc::Row &p) const noexcept {
            ovc::row_num_calls_to_hash++;
            return p.key;
        }
    };
}