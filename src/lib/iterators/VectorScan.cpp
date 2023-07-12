#include "VectorScan.h"

#include <utility>

namespace ovc::iterators {

    VectorScan::VectorScan(std::vector<Row> rows) : rows(std::move(rows)), index(0) {
    }

    Row *VectorScan::next() {
        Iterator::next();
        if (index >= rows.size()) {
            return nullptr;
        }
        return &rows[index++];
    }
}