#pragma once

#include <cstring>
#include "Row.h"
#include "lib/io/ExternalRunW.h"

namespace ovc {
    using namespace ovc::io;

    struct hash_set_stats {
        size_t hash_comparisons;
        size_t row_comparisons;
    };

    extern struct hash_set_stats hs_stats;

    static inline void hash_set_stats_reset() {
        memset(&hs_stats, 0, sizeof hs_stats);
    }

    class Partitioner {
    public:
        explicit Partitioner(int num_partitions);

        ~Partitioner();

        /**
         * Insert a row into its partition. The Row's `key` field is used for its hash value.
         * @param row The row to be inserted.
         */
        void put(Row *row);

        void finalize();

        std::vector<std::string> getPartitions();

    private:
        int num_partitions;
        std::vector<std::string> paths;
        std::vector<ExternalRunW> partitions;
        BufferManager bufferManager;
    };
}