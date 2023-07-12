#include "lib/io/ExternalRunRS.h"
#include "lib/log.h"

#include <cstdio>

int main(int argc, char **argv) {
    ovc::log_set_quiet(true);
    for (int i = 1; i < argc; i++) {
        ovc::io::ExternalRunRS run(argv[i]);
        size_t count;
        for (count = 0; run.read() != nullptr; count++) {}
        printf("%zu\n", count);
    }
}