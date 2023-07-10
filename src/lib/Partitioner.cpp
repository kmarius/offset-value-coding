#include "Partitioner.h"
#include "log.h"

struct hash_set_stats hs_stats = {0};

static std::string gen_path(int i) {
    return BASEDIR "/part" + std::to_string(i) + ".dat";
}

Partitioner::Partitioner(int num_partitions)
        : num_partitions(num_partitions), bufferManager(num_partitions << 1) {
    assert(num_partitions > 0);
    log_info("num_partitions=%d", num_partitions);

    partitions.reserve(num_partitions);
    for (int i = 0; i < num_partitions; i++) {
        std::string path = gen_path(i);
        paths.push_back(path);
        partitions.emplace_back(path, bufferManager);
    }
}

Partitioner::~Partitioner() {
    //delete[] partitions;
}

void Partitioner::put(Row *row) {
    return partitions[row->key % num_partitions].add(*row);
}

std::vector<std::string> Partitioner::getPartitions() {
    return paths;
}

void Partitioner::finalize() {
    for (int i = 0; i < num_partitions; i++) {
        partitions[i].finalize();
    }
}