#pragma once

#include <vector>
#include <algorithm>

#include "Row.h"

/**
 * This in-memory run holds _pointers_ to rows, which, of course, should outlive this run.
 */

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
    bool isSorted() {
        for (int i = 1; i < data.size(); i++) {
            if (data[i]->less(*data[i - 1])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the very first row of the run.
     * @return The first row of the run.
     */
    Row *front() {
        assert(!data.empty());
        return data.front();
    }

    /**
     * Set the offset value codes in a sorted run.
     */
    void setOvcs() {
        if (!data.empty()) {
            data[0]->key = DOMAIN * ROW_ARITY + data[0]->columns[0];
            for (size_t i = 1; i < data.size(); i++) {
                OVC ovc;
                data[i]->less(*data[i + 1], ovc);
                data[i + 1]->key = ovc;
            }
        }
    }
};

