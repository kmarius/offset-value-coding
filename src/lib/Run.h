#pragma once

#include <vector>
#include <algorithm>

#include "Row.h"

/**
 * This in-memory run holds _pointers_ to rows, which, of course, should outlive this run.
 */

namespace ovc {

    struct MemoryRun {
        std::vector<Row *> data;
        size_t ind;

    public:
        MemoryRun() : ind(0) {}

        /**
         * Reserve space in the underlying vector.
         * @param size
         */
        void reserve(size_t size) {
            data.reserve(size);
        }

        /**
         * Add a row to the run.
         * @param row The row.
         */
        void add(Row *row) {
            data.push_back(row);
        }

        /**
         * Read the next row from the run. The pointer is only valid until the next call to next().
         * Must not be called on an empty run.
         * @return The next row.
         */
        Row *next() {
            assert(ind < data.size());
            return data[ind++];
        }

        /**
         * The size (i.e. number of remaining rows) in the run.
         * @return The number of remaining rows.
         */
        size_t size() const {
            return data.size() - ind;
        }

        /**
         * Check if the run is isEmpty.
         * @return True if the run is isEmpty. False otherwise.
         */
        bool isEmpty() const {
            return size() == 0;
        }

        /**
         * Sort the run with quicksort.
         */
        void sort() {
            std::sort(data.begin(), data.end(),
                      [](const Row *a, const Row *b) -> bool {
                          return a->less(*b);
                      });
        }

        /**
         * Check if the run is sorted.
         * @return True if the run is sorted. False otherwise.
         */
         template<typename Compare = RowCmp>
        bool isSorted(const Compare &cmp = RowCmp{}) {
            for (int i = 1; i < data.size(); i++) {
                if (cmp(*data[i], *data[i - 1]) < 0) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Peek the next row of the run.
         * @return The next row of the run.
         */
        Row *front() {
            if (ind >= data.size()) {
                return nullptr;
            }
            assert(ind < data.size());
            return data[ind];
        }

        Row *back() {
            assert(data.size() > 0);
            return data.back();
        }

        /**
         * Set the offset value codes in a sorted run.
         */
        void setOvcs() {
            if (!data.empty()) {
                data[0]->key = DOMAIN * ROW_ARITY + data[0]->columns[0];
                for (size_t i = 1; i < data.size(); i++) {
                    OVC ovc;
                    data[i]->cmp(*data[i + 1], ovc);
                    data[i + 1]->key = ovc;
                }
            }
        }
    };
}