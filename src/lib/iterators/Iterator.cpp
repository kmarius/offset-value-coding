#include "Iterator.h"
#include "lib/log.h"
#include "lib/io/ExternalRunW.h"

void Iterator::run(bool print) {
    Count rows_seen = 0;

    open();
    for (Row *row; (row = next()); free()) {
        ++rows_seen;
        if (print) {
            log_info("%s", row->c_str());
        }
    }
    close();

    log_info("%lu rows seen", (unsigned long) rows_seen);
}

std::vector<Row> Iterator::collect() {
    std::vector<Row> rows = {};
    open();
    for (Row *row; (row = next()); free()) {
        rows.push_back(*row);
    }
    close();
    return rows;
}

void Iterator::write(const std::string &path) {
    BufferManager manager(2);
    ExternalRunW run(path, manager);
    open();
    for (Row *row; (row = next()); free()) {
        run.add(*row);
    }
    close();
}

