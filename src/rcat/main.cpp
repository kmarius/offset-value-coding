#include "lib/io/ExternalRunRS.h"
#include "lib/log.h"

#include <cstdio>

int main(int argc, char **argv) {
    log_set_quiet(true);
    for (int i = 1; i < argc; i++) {
        printf("%s\n", argv[i]);
        ExternalRunRS run(argv[i]);
        Row *row;
        int count;

        for (count = 0; (row = run.read()) != nullptr; count++) {
            printf("%4d %s\n", count, row->c_str());
        }
        printf("%d rows read\n", count);
    }
}