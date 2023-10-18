#include "Iterator.h"
#include "lib/log.h"
#include "lib/io/ExternalRunW.h"

namespace ovc::iterators {

    void Iterator::run(bool print) {
        Count rows_seen = 0;

        for (auto &row : *this) {
            rows_seen++;
            if (print) {
                log_info("%s", row.c_str());
            }
        }

        log_info("%lu rows seen", (unsigned long) rows_seen);
    }

    std::vector<Row> Iterator::collect() {
        std::vector<Row> rows = {};

        for (auto &row : *this) {
            rows.push_back(row);
        }

        return rows;
    }

    void Iterator::write(const std::string &path) {
        ovc::io::BufferManager manager(2);
        ovc::io::ExternalRunW run(path, manager);
        for (auto &row : *this) {
            run.add(row);
        }
    }
}