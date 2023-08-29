#include "utils.h"
#include "defs.h"

namespace ovc {
    std::string generate_path() {
        static int i = 0;
        return std::string(BASEDIR "/file" + std::to_string(i++) + ".dat");
    }
};