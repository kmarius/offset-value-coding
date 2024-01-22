#pragma once

#include "Row.h"

namespace ovc::comparators {

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

        long raw(const ovc::Row &lhs, const ovc::Row &rhs) const {
            for (int i = 0; i < length; i++) {
                uint8_t ind = columns[i];
                long cmp = (long) lhs.columns[ind] - (long) rhs.columns[ind];
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

                // if i > prefix, rows are equal
                int i = lhs.getOVC().getOffset() + 1;
                for (; i < length; i++) {
                    cmp = (long) lhs.columns[columns[i]] - (long) rhs.columns[columns[i]];
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

        long raw(const ovc::Row &lhs, const ovc::Row &rhs) const {
            for (int i = 0; i < length; i++) {
                uint8_t ind = columns[i];
                long cmp = (long) lhs.columns[ind] - (long) rhs.columns[ind];
                if (cmp != 0) {
                    return cmp;
                }
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

        EqColumnList(uint8_t *columns, int n, iterator_stats *stats = nullptr)
                : length(n), stats(stats), columns() {
            assert(n <= ROW_ARITY);
            for (int i = 0; i < n; i++) {
                this->columns[i] = columns[i];
            }
        };

        bool operator()(const ovc::Row &lhs, const ovc::Row &rhs) const {
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
}