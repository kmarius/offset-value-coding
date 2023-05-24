#include "lib/io/ExternalRunRS.h"

#include <cstdio>

int main(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        ExternalRunRS run(argv[i]);
        size_t count;
        for (count = 0; run.read() != nullptr; count++) {}
        printf("%zu\n", count);
    }
}