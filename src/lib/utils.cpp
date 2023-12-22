#include "utils.h"
#include "defs.h"

#include <unistd.h>

namespace ovc {
    std::string generate_path() {
        static int i = 0;
        static int pid = getpid();
        return std::string(BASEDIR "/ovc." + std::to_string(pid) + "." + std::to_string(i++) + ".dat");
    }
};