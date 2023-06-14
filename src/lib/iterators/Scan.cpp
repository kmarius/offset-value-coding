#include "Scan.h"

Scan::Scan(const std::string &path) : buffer_manager(1), run(path, buffer_manager) {
}

Row *Scan::next() {
    Iterator::next();
    return run.read();
}