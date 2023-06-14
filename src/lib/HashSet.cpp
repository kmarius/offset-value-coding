#include "HashSet.h"
#include "log.h"

struct hash_set_stats hs_stats = {0};

struct HashSet::bucket {
    bucket() : rows({}), index(0) {}

    std::vector<Row> rows;
    size_t index;

    bool put(Row *row) {
        for (auto &other: rows) {
            // hash comparison:
            hs_stats.hash_comparisons++;
            if (other.key == row->key) {
                hs_stats.row_comparisons++;
                // column comparison:
                if (other.equals(*row)) {
                    log_trace("dupe found by column comparison: ");
                    log_trace("%s and", row->c_str());
                    log_trace("%s", other.c_str());
                    return false;
                }
            }
        }
        rows.push_back(*row);
        return true;
    }

    Row *next() {
        if (index >= rows.size()) {
            return nullptr;
        }
        return &rows[index++];
    }

    bool isEmpty() const {
        return index >= rows.size();
    }
};

HashSet::HashSet(int num_buckets) : num_buckets(num_buckets), next_bucket(0) {
    assert(num_buckets > 0);
    buckets = new bucket[num_buckets];
    log_info("num_buckets=%d", num_buckets);
}

HashSet::~HashSet() {
    delete[] buckets;
}

Row *HashSet::next() {
    while (next_bucket < num_buckets && buckets[next_bucket].isEmpty()) {
        next_bucket++;
    }
    if (next_bucket >= num_buckets) {
        return nullptr;
    }

    return buckets[next_bucket].next();
}

bool HashSet::put(Row *row) {
    return buckets[row->key % num_buckets].put(row);
}