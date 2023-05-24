#include "Scan.h"

Scan::Scan(const std::string &path) : buffer_manager(1), run(path, buffer_manager) {
}

Scan::~Scan() {
    assert(status == Closed);
};

void Scan::open() {
    assert(status == Unopened);
    status = Opened;
}

void Scan::close() {
    assert(status == Opened);
    status = Closed;
}

Row *Scan::next() {
    assert(status == Opened);
    return run.read();
}

void Scan::free() {}