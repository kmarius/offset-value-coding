#include "lib/io/ExternalRunRS.h"

#include <cstdio>

int main(int argc, char **argv) {
    int fail = 0;
    for (int i = 1; i < argc; i++) {
        ExternalRunRS run(argv[i]);

        Row *row, prev;
        if ((row = run.read()) == nullptr) {
            continue;
        }
        prev = *row;

        int count;
        for (count = 1; (row = run.read()) != nullptr; count++, prev = *row) {
            if (row->less(prev)) {
                printf("row at index=%d is smaller than the previous\n", i);
                printf("prev: %s\n", prev.c_str());
                printf("cur:  %s\n", row->c_str());
                fail++;
                break;
            }
        }
    }
    return fail;
}