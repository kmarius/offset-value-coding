#include "VectorScan.h"

#include <utility>

VectorScan::VectorScan(std::vector<Row> rows) : rows(std::move(rows)), index(0) {

}

VectorScan::~VectorScan() {
    assert(status == Closed);
}

void VectorScan::open() {
    assert(status == Unopened);
    status = Opened;
}

Row *VectorScan::next() {
    assert(status == Opened);
    if (index >= rows.size()) {
        return nullptr;
    }
    return &rows[index++];
}

void VectorScan::free() {}

void VectorScan::close() {
    assert(status == Opened);
    status = Closed;
}
