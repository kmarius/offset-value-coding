#include "lib/iterators/Generator.h"
#include "lib/log.h"

#include <cstdlib>

// TODO: add seed argument
// TODO: once we support different schemas, allow specification via arguments

int main(int argc, char *argv[]) {
    ovc::log_set_quiet(true);
    if (argc < 3) {
        fprintf(stderr, "usage: %s <path_sync> <n>\n", argv[0]);
        return 1;
    }
    const char *path = argv[1];
    char *end;
    long num_rows = strtol(argv[2], &end, 10);
    if (num_rows <= 0) {
        fprintf(stderr, "count_ must be positive\n");
        return 1;
    }

    ovc::iterators::Generator gen(num_rows, 1000);
    gen.write(path);

    return 0;
}