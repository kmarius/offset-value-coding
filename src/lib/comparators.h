#pragma once

#include "Row.h"

namespace ovc::comparators {

    struct CmpColumnListCool {
        uint8_t columns[ROW_ARITY];
        int length;
        int lengthC;
        struct iterator_stats *stats;
        static const bool USES_OVC = false;

        CmpColumnListCool addStats(struct iterator_stats *stats) const {
            CmpColumnListCool cmp;
            cmp.stats = stats;
            cmp.length = length;
            cmp.lengthC = lengthC;
            mempcpy(cmp.columns, columns, length * sizeof *columns);
            return cmp;
        }

        CmpColumnListCool append(uint8_t *columns_, int length_) const {
            CmpColumnListCool cmp;
            cmp.stats = stats;
            cmp.length = length + length_;
            cmp.lengthC = lengthC;
            mempcpy(cmp.columns, columns, length * sizeof *columns);
            mempcpy(cmp.columns + length, columns_, length_ * sizeof *columns_);
            return cmp;
        }

        explicit CmpColumnListCool(std::initializer_list<uint8_t> columns, iterator_stats *stats = nullptr)
                : length(columns.size()), stats(stats), lengthC(columns.size()) {
            assert(columns.size() <= ROW_ARITY);
            int i = 0;
            for (auto col: columns) {
                this->columns[i++] = col;
            }
        };

        CmpColumnListCool(const uint8_t *cols, int n, iterator_stats *stats = nullptr)
                : length(n), stats(stats), lengthC(n) {
            assert(n <= ROW_ARITY);
            for (int i = 0; i < n; i++) {
                columns[i] = cols[i];
            }
        };

        long operator()(const ovc::Row &lhs, const ovc::Row &rhs) const {
            long cmp;
            int i = 0;
            for (; i < lengthC; i++) {
                cmp = (long) lhs.columns[columns[i]] - (long) rhs.columns[columns[i]];

#ifdef COLLECT_STATS
                if (stats) {
                    log_trace("inc %lu %p", stats->column_comparisons, stats);
                    stats->column_comparisons++;
                }
#endif
                if (cmp) {
                    break;
                }
            }

            if (i < lengthC) {
                // difference in C
                return cmp;
            }

            // Equality on AC, derive the offset-value code from stored_ovcs
            int ind_l = lhs.tid;
            int ind_r = rhs.tid;
            assert(ind_l - ind_r);
            return ind_l - ind_r;
        }

        long raw(const ovc::Row &lhs, const ovc::Row &rhs) const {
            for (int i = 0; i < length; i++) {
                long cmp = (long) lhs.columns[columns[i]] - (long) rhs.columns[columns[i]];
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        inline unsigned long makeOVC(long arity, long offset, const ovc::Row *row) const {
            log_error("makeOVC called on CmpPrefix");
            assert(false);
            return 0;
        }

    private:
        CmpColumnListCool() : columns(), length(0), stats(nullptr) {};
    };

    struct CmpColumnList {
        uint8_t columns[ROW_ARITY];
        int length;
        struct iterator_stats *stats;
        static const bool USES_OVC = false;

        CmpColumnList addStats(struct iterator_stats *stats) const {
            CmpColumnList cmp;
            cmp.stats = stats;
            cmp.length = length;
            mempcpy(cmp.columns, columns, length * sizeof *columns);
            return cmp;
        }

        CmpColumnList append(uint8_t *columns_, int length_) const {
            CmpColumnList cmp;
            cmp.stats = stats;
            cmp.length = length + length_;
            mempcpy(cmp.columns, columns, length * sizeof *columns);
            mempcpy(cmp.columns + length, columns_, length_ * sizeof *columns_);
            return cmp;
        }

        explicit CmpColumnList(std::initializer_list<uint8_t> columns, iterator_stats *stats = nullptr)
                : length(columns.size()), stats(stats) {
            assert(columns.size() <= ROW_ARITY);
            int i = 0;
            for (auto col: columns) {
                this->columns[i++] = col;
            }
        };

        CmpColumnList(const uint8_t *cols, int n, iterator_stats *stats = nullptr)
                : length(n), stats(stats) {
            assert(n <= ROW_ARITY);
            for (int i = 0; i < n; i++) {
                columns[i] = cols[i];
            }
        };

        long operator()(const ovc::Row &lhs, const ovc::Row &rhs) const {
            for (int i = 0; i < length; i++) {

#ifdef COLLECT_STATS
                if (stats) {
                    stats->column_comparisons++;
                }
#endif
                long cmp = (long) lhs.columns[columns[i]] - (long) rhs.columns[columns[i]];
                if (cmp) {
                    return cmp;
                }
            }
            return 0;
        }

        long raw(const ovc::Row &lhs, const ovc::Row &rhs) const {
            for (int i = 0; i < length; i++) {
                long cmp = (long) lhs.columns[columns[i]] - (long) rhs.columns[columns[i]];
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        inline unsigned long makeOVC(long arity, long offset, const ovc::Row *row) const {
            log_error("makeOVC called on CmpPrefix");
            assert(false);
            return 0;
        }

    private:
        CmpColumnList() : columns(), length(0), stats(nullptr) {};
    };

    struct CmpPrefix : public CmpColumnList {
    public:
        CmpPrefix() = delete;

        explicit CmpPrefix(int prefix, iterator_stats *stats = nullptr)
                : CmpColumnList(nullptr, 0, stats) {
            length = prefix;
            for (int i = 0; i < prefix; i++) {
                columns[i] = i;
            }
        };
    };

    struct Cmp : public CmpPrefix {
    public:
        explicit Cmp(iterator_stats *stats = nullptr) : CmpPrefix(ROW_ARITY, stats) {}
    };

    struct CmpColumnListOVC {
        uint8_t columns[ROW_ARITY];
        int length;
        struct iterator_stats *stats;
        static const bool USES_OVC = true;
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

        CmpColumnListOVC(const uint8_t *cols, int n, iterator_stats *stats = nullptr)
                : length(n), stats(stats) {
            assert(n <= ROW_ARITY);
            for (int i = 0; i < n; i++) {
                columns[i] = cols[i];
            }
        };

        inline unsigned long makeOVC(long arity, long offset, const ovc::Row *row) const {
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

        long operator()(ovc::Row &lhs, ovc::Row &rhs) const {
            long cmp = (long) lhs.key - (long) rhs.key;
            if (cmp == 0) {
                if (lhs.key == 0) {
                    // equality, no need to go on
                    return 0;
                }
                // if the offset is |AC|-1 (i.e. the last column of C) we can derive the offset-value code
                // by taking the maximum ovc of the first rows all runs between the run of lhs, rhs

                // if i > prefix, rows are equal
                int i = lhs.getOffset() + 1;
                for (; i < length; i++) {
                    cmp = (long) lhs.columns[columns[i]] - (long) rhs.columns[columns[i]];

#ifdef COLLECT_STATS
                    if (stats) {
                        stats->column_comparisons++;
                    }
#endif
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

        long makeInitialOVC(const ovc::Row &row) {
            return MAKE_OVC(ROW_ARITY, 0, row.columns[columns[0]]);
        }

        long raw(const ovc::Row &lhs, const ovc::Row &rhs, unsigned long *ovc = nullptr) const {
            for (int i = 0; i < length; i++) {
                uint8_t ind = columns[i];
                long cmp = (long) lhs.columns[ind] - (long) rhs.columns[ind];
                if (cmp != 0) {
                    if (ovc) {
                        if (cmp < 0) {
                            *ovc = MAKE_OVC(ROW_ARITY, i, rhs.columns[ind]);
                        } else {
                            *ovc = MAKE_OVC(ROW_ARITY, i, lhs.columns[ind]);
                        }
                    }
                    return cmp;
                }
            }
            if (ovc) {
                *ovc = 0;
            }
            return 0;
        }
    };

    struct CmpPrefixOVC : public CmpColumnListOVC {
    public:
        CmpPrefixOVC() = delete;

        explicit CmpPrefixOVC(int prefix, iterator_stats *stats = nullptr)
                : CmpColumnListOVC(nullptr, 0, stats) {
            length = prefix;
            for (int i = 0; i < prefix; i++) {
                columns[i] = i;
            }
        };
    };

    struct CmpOVC : public CmpPrefixOVC {
    public:
        explicit CmpOVC(iterator_stats *stats = nullptr) : CmpPrefixOVC(ROW_ARITY, stats) {}
    };

    struct CmpColumnListDerivingOVC {
        uint8_t columns[ROW_ARITY];
        int length;
        struct iterator_stats *stats;
        static const bool USES_OVC = true;
        OVC *stored_ovcs;
        int length_ac;
        int length_c;
    private:
        CmpColumnListDerivingOVC() : columns(), length(0), stats(nullptr), stored_ovcs(nullptr) {};

    public:
        CmpColumnListDerivingOVC addStats(struct iterator_stats *stats) const {
            CmpColumnListDerivingOVC cmp;
            cmp.stats = stats;
            cmp.length = length;
            cmp.length_c = length_c;
            cmp.length_ac = length_ac;
            mempcpy(cmp.columns, columns, length * sizeof *columns);
            return cmp;
        }

        CmpColumnListDerivingOVC transposeBC() const {
            CmpColumnListDerivingOVC cmp;
            cmp.stats = stats;
            cmp.length = length;
            int lA = length_ac - length_c;
            int lB = length_c;
            int lC = length - lA - lB;
            cmp.length_c = lC;
            cmp.length_ac = lA + lC;
            for (int i = 0; i < lA; i++) {
                cmp.columns[i] = columns[i];
            }
            for (int i = 0; i < lC; i++) {
                cmp.columns[lA + i] = columns[lA + lB + i];
            }
            for (int i = 0; i < lB; i++) {
                cmp.columns[lA + lC + i] = columns[lA + i];
            }
            return cmp;
        }

        CmpColumnListDerivingOVC(const uint8_t *cols, int a, int b, int c, iterator_stats *stats = nullptr)
                : length(a + b + c), stats(stats), length_ac(a + b), length_c(b) {
            assert(length <= ROW_ARITY);
            for (int i = 0; i < length; i++) {
                columns[i] = cols[i];
            }
        };

        CmpColumnListDerivingOVC(std::initializer_list<uint8_t> columns, int ac, int c,
                                 iterator_stats *stats = nullptr)
                : length(columns.size()), stats(stats), length_ac(ac), length_c(c) {
            assert(columns.size() <= ROW_ARITY);
            int i = 0;
            for (auto col: columns) {
                this->columns[i++] = col;
            }
        };

        inline unsigned long makeOVC(long arity, long offset, const ovc::Row *row) const {
            return MAKE_OVC(arity, offset, row->columns[columns[offset]]);
        }

        long operator()(ovc::Row &lhs, ovc::Row &rhs) const {
            long cmp = (long) lhs.key - (long) rhs.key;
            if (cmp) {
                return cmp;
            }
            if (lhs.key == 0) {
                // equality, no need to go on
                return 0;
            }
            char buf[128];
            log_trace("%s, %s", lhs.c_str(), rhs.c_str(buf));

            int offset = lhs.getOffset();

            int i = offset + 1;
            if (i < length_ac) {
                log_trace("checking for difference in AC");
            }
            for (; i < length_ac; i++) {
                cmp = (long) lhs.columns[columns[i]] - (long) rhs.columns[columns[i]];

#ifdef COLLECT_STATS
                if (stats) {
                    log_trace("inc %lu %p", stats->column_comparisons, stats);
                    stats->column_comparisons++;
                }
#endif
                if (cmp) {
                    break;
                }
            }
            if (i < length_ac) {
                // rows differ within AC
                if (cmp <= 0) {
                    rhs.key = MAKE_OVC(ROW_ARITY, i, rhs.columns[columns[i]]);
                } else {
                    lhs.key = MAKE_OVC(ROW_ARITY, i, lhs.columns[columns[i]]);
                }
                return cmp;
            } else {
                // Equality on AC, derive the offset-value code from stored_ovcs
                int ind_l = lhs.tid;
                int ind_r = rhs.tid;
                if (ind_l < ind_r) {
                    unsigned long max = 0;
                    for (int j = ind_l + 1; j <= ind_r; j++) {
                        if (stored_ovcs[j] > max) {
                            max = stored_ovcs[j];
                        }
                    }
                    // increment offset of max by |C|
                    max = OVC_SET_OFFSET(max, OVC_GET_OFFSET(max, ROW_ARITY) + length_c, ROW_ARITY);
                    rhs.key = max;
                    return -1;
                } else {
                    unsigned long max = 0;
                    for (int j = ind_r + 1; j <= ind_l; j++) {
                        if (stored_ovcs[j] > max) {
                            max = stored_ovcs[j];
                        }
                    }
                    // increment offset of max by |C|
                    max = OVC_SET_OFFSET(max, OVC_GET_OFFSET(max, ROW_ARITY) + length_c, ROW_ARITY);
                    lhs.key = max;
                    return 1;
                }
            }
        }
    };

    struct EqColumnList {
        uint8_t columns[ROW_ARITY];
        int length;
        struct iterator_stats *stats;
        static const bool USES_OVC = false;
    public:
        explicit EqColumnList(std::initializer_list<uint8_t> columns, iterator_stats *stats = nullptr)
                : length(columns.size()), stats(stats) {
            assert(columns.size() <= ROW_ARITY);
            int i = 0;
            for (auto col: columns) {
                this->columns[i++] = col;
            }
        };

        EqColumnList(const uint8_t *columns, int n, iterator_stats *stats = nullptr)
                : length(n), stats(stats), columns() {
            assert(n <= ROW_ARITY);
            for (int i = 0; i < n; i++) {
                this->columns[i] = columns[i];
            }
        };

        inline EqColumnList addStats(struct iterator_stats *stats) const {
            return EqColumnList((uint8_t *) columns, length, stats);
        }

        bool operator()(const ovc::Row &lhs, const ovc::Row &rhs) const {
            for (int i = 0; i < length; i++) {
#ifdef COLLECT_STATS
                if (stats) {
                    stats->column_comparisons++;
                }
#endif
                if (lhs.columns[columns[i]] != rhs.columns[columns[i]]) {
                    return false;
                };
            }
            return true;
        }

        bool raw(const ovc::Row &lhs, const ovc::Row &rhs) const {
            for (int i = 0; i < length; i++) {
                if (lhs.columns[columns[i]] != rhs.columns[columns[i]]) {
                    return false;
                };
            }
            return true;
        }
    };

    struct EqPrefix : public EqColumnList {
    public:
        EqPrefix() = delete;

        explicit EqPrefix(int prefix, iterator_stats *stats = nullptr)
                : EqColumnList(nullptr, 0, stats) {
            length = prefix;
            for (int i = 0; i < prefix; i++) {
                columns[i] = i;
            }
        };
    };

    struct Eq : public EqPrefix {
    public:
        explicit Eq(iterator_stats *stats = nullptr) : EqPrefix(ROW_ARITY, stats) {};
    };

    struct EqColumnListOVC {
        uint8_t columns[ROW_ARITY];
        int length;
        struct iterator_stats *stats;
        static const bool USES_OVC = true;
    public:
        explicit EqColumnListOVC(std::initializer_list<uint8_t> columns, iterator_stats *stats = nullptr)
                : length(columns.size()), stats(stats), columns() {
            assert(columns.size() <= ROW_ARITY);
            int i = 0;
            for (auto c: columns) {
                this->columns[i++] = c;
            }
        }

        EqColumnListOVC(uint8_t *columns, int n, iterator_stats *stats = nullptr)
                : length(n), stats(stats) {
            assert(n <= ROW_ARITY);
            for (int i = 0; i < n; i++) {
                this->columns[i] = columns[i];
            }
        };

        inline EqColumnListOVC addStats(struct iterator_stats *stats) const {
            return EqColumnListOVC((uint8_t *) columns, length, stats);
        }

        bool operator()(const ovc::Row &next, const ovc::Row &prev) const {
            return next.key == 0;
        }

        bool raw(const ovc::Row &lhs, const ovc::Row &rhs) const {
            for (int i = 0; i < length; i++) {
                if (lhs.columns[columns[i]] != rhs.columns[columns[i]]) {
                    return false;
                };
            }
            return true;
        }
    };

    struct EqPrefixOVC : public EqColumnListOVC {
    public:
        EqPrefixOVC() = delete;

        explicit EqPrefixOVC(int prefix, iterator_stats *stats = nullptr)
                : EqColumnListOVC(nullptr, 0, stats) {
            length = prefix;
            for (int i = 0; i < prefix; i++) {
                columns[i] = i;
            }
        };
    };

    struct EqOVC : public EqPrefixOVC {
    public:
        explicit EqOVC(iterator_stats *stats = nullptr) : EqPrefixOVC(ROW_ARITY, stats) {};
    };

    struct EqOffset {
        const int arity; /* arity of the present OVCs */
        const int offset; /* length of the prefix we are checking for equality */
        static const bool USES_OVC = true;
        uint8_t columns[ROW_ARITY];
        struct iterator_stats *stats;

        explicit EqOffset(std::initializer_list<uint8_t> columns, int offset, iterator_stats *stats = nullptr)
                : arity(ROW_ARITY), stats(stats), columns(), offset(offset) {
            assert(columns.size() <= ROW_ARITY);
            int i = 0;
            for (auto c: columns) {
                this->columns[i++] = c;
            }
        }

        EqOffset(const uint8_t *columns, int n, int offset, iterator_stats *stats = nullptr)
                : arity(ROW_ARITY), stats(stats), offset(offset), columns() {
            assert(n <= ROW_ARITY);
            for (int i = 0; i < n; i++) {
                this->columns[i] = columns[i];
            }
        };

        inline EqOffset addStats(struct iterator_stats *stats) const {
            return EqOffset((uint8_t *) columns, arity, offset, stats);
        }

        bool operator()(const ovc::Row &next, const ovc::Row &prev) const {
            return next.getOffset(arity) >= offset;
        }

        bool raw(const ovc::Row &lhs, const ovc::Row &rhs) const {
            assert(false);
            return false;
        }
    };
}