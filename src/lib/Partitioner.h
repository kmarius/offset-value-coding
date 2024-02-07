#pragma once

#include <cstring>
#include "Row.h"
#include "lib/io/ExternalRunW.h"
#include "log.h"
#include "utils.h"

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
        explicit Partitioner(int num_partitions)
                : num_partitions(num_partitions), bufferManager(num_partitions << 1), stats(), finalized(false) {
            assert(num_partitions > 0);

            partitions.reserve(num_partitions);
            for (int i = 0; i < num_partitions; i++) {
                std::string path = generate_path();
                partitions.emplace_back(path, bufferManager, true);
            }
        };

        ~Partitioner() {
            finalize();
        };

        /**
         * Insert a row into its partition. The Row's `key` field is used for its hash getValue.
         * @param row The row to be inserted.
         */
        void put(Row *row) {
            stats.rows_written++;
            auto hash = row->key;
            row->key = (hash << 8) | (hash >> ((8 * sizeof hash) - 8));
            partitions[hash % num_partitions].add(*row);
        };

        /**
         * Perform early aggregation in the buffer page of the corresponding bucket that is prev in memory.
         */
        template<typename Aggregate, typename Equal>
        void putEarlyAggregate(Row *row, Aggregate &agg, Equal &eq) {
            auto hash = row->key;
            row->key = (hash << 8) | (hash >> ((8 * sizeof hash) - 8));
            auto &part = partitions[hash % num_partitions];
            Row *acc = part.probe_page(row, eq);
            if (acc) {
                agg.merge(*acc, *row);
            } else {
                part.add(*row);
            }
        };

        /**
         * Insert only if it is not already contained in the current page
         */
        template<typename Equal>
        bool putDistinct(Row *row, Equal &eq) {
            auto hash = row->key;
            row->key = (hash << 8) | (hash >> ((8 * sizeof hash) - 8));
            auto &part = partitions[hash % num_partitions];
            if (part.probe_page(row, eq)) {
                return false;
            }
            part.add(*row);
            return true;
        };

        /**
         * Finalize only buckets that have spilled to disk. Returns a vector of the rows that were not spilled.
         */
        template<typename Aggregate>
        std::vector<Row> finalizeEarlyAggregate(Aggregate &agg) {
            std::vector<Row> rows;
            std::vector<std::string> paths;
            for (auto &partition: partitions) {
                if (partition.didSpill()) {
                    stats.rows_written += partition.size();
                    partition.finalize();
                    paths.push_back(partition.path());
                } else {
                    // collect in-memory rows
                    const Row *end = partition.end_page();
                    for (Row *it = partition.begin_page(); it < end; it++) {
                        agg.finalize(*it);
                        rows.push_back(*it);
                    }
                    partition.discard();
                }
            }
            this->paths = paths;
            partitions.clear();
            return rows;
        }
        /**
         * Finalize only buckets that have spilled to disk. Returns a vector of the rows that were not spilled.
         */
        std::vector<Row> finalizeDistinct() {
            std::vector<Row> rows;
            std::vector<std::string> paths;
            for (auto &partition: partitions) {
                if (partition.didSpill()) {
                    stats.rows_written += partition.size();
                    partition.finalize();
                    paths.push_back(partition.path());
                } else {
                    // collect in-memory rows
                    const Row *end = partition.end_page();
                    for (Row *it = partition.begin_page(); it < end; it++) {
                        rows.push_back(*it);
                    }
                    partition.discard();
                }
            }
            this->paths = paths;
            partitions.clear();
            return rows;
        }

        /**
         * Finalize all buckets.
         */
        void finalize(bool keep = false) {
            if (!finalized) {
                finalized = true;
                std::vector<std::string> newPaths;
                for (auto &partition: partitions) {
                    if (partition.size() == 0) {
                        partition.discard();
                        if (keep) {
                            newPaths.push_back(partition.path());
                        }
                    } else {
                        newPaths.push_back(partition.path());
                        partition.finalize();
                    }
                }
                partitions.clear();
                this->paths = newPaths;
            }
        };

        std::vector<std::string> getPartitionPaths() {
            return paths;
        };

        struct iterator_stats &getStats() {
            return stats;
        }

    private:
        int num_partitions;
        std::vector<std::string> paths;
        std::vector<ExternalRunW> partitions;
        BufferManager bufferManager;
        struct iterator_stats stats;
        bool finalized;
    };
}