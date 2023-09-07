#include "lib/io/ExternalRunRS.h"
#include "lib/log.h"

#include <cstdio>

int main(int argc, char **argv) {
    ovc::log_set_quiet(true);
    int fail = 0;
    for (int i = 1; i < argc; i++) {
        ovc::io::ExternalRunRS run(argv[i]);

        ovc::Row *row, prev;
        if ((row = run.read()) == nullptr) {
            continue;
        }
        prev = *row;
        ovc::RowCmp cmp;

        int count;
        for (count = 1; (row = run.read()) != nullptr; count++, prev = *row) {
            if (cmp(*row, prev) < 0) {
                fprintf(stderr, "row at index=%d is smaller than the previous\n", i);
                fprintf(stderr, "prev: %s\n", prev.c_str());
                fprintf(stderr, "cur:  %s\n", row->c_str());
                fail++;
                break;
            }
        }
    }
    return fail;
}