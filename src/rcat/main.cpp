#include "lib/io/ExternalRunRS.h"
#include "lib/log.h"

#include <cstdio>

int main(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        printf("%s\n", argv[i]);
        ovc::io::ExternalRunRS run(argv[i]);
        ovc::Row *row;
        int count;

        for (count = 0; (row = run.read()) != nullptr; count++) {
            printf("%4d %s\n", count, row->c_str());
        }
        printf("%d rows read\n", count);
    }
}