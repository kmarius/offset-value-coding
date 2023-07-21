#include "HashGroupBy.h"
#include "lib/Partitioner.h"
#include "lib/log.h"

namespace ovc::iterators {

    HashGroupBy::HashGroupBy(Iterator *input, int group_columns)
            : UnaryIterator(input), group_columns(group_columns), ind(0) {
        output_is_unique = true;
    }

    void HashGroupBy::open() {
        Iterator::open();
        input_->open();

        Partitioner partitioner(1 << RUN_IDX_BITS);

        for (Row *row; (row = input_->next()); input_->free()) {
            row->setHash(group_columns);
            partitioner.put(row);
        }
        partitioner.finalize();
        partitions = partitioner.getPartitions();
        rows = process_partition(partitions.back());
        input_->close();
    }

    void HashGroupBy::close() {
        Iterator::close();
        for (auto &path: partitions) {
            ExternalRunR(path, bufferManager, true).remove();
        }
    }

    Row *HashGroupBy::next() {
        if (partitions.empty()) {
            return nullptr;
        }

        if (ind >= rows.size()) {
            rows = {};
            ind = 0;
        }

        while (rows.empty()) {
            partitions.pop_back();
            if (partitions.empty()) {
                return nullptr;
            }
            rows = process_partition(partitions.back());
        }

        return &rows[ind++];
    }

    std::vector<Row> HashGroupBy::process_partition(const std::string &path) {
        auto res = std::vector<Row>();

        ExternalRunR part(path, bufferManager, true);

        auto counts = std::unordered_map<Row, unsigned long, std::hash<Row>, RowEqualPrefix>(128, std::hash<Row>(), RowEqualPrefix(group_columns));
        static int part_idx = 0;
        part_idx++;

        for (Row *row; (row = part.read());) {
            auto tup = counts.find(*row);
            if (tup == counts.end()) {
                counts[*row] = 1;
            } else {
                tup->second++;
            }
        }

        part.remove();

        for (auto it: counts) {
            res.push_back(it.first);
            res.back().key = 0;
            res.back().tid = 0;
            res.back().columns[group_columns] = it.second;
            for (int i = group_columns + 1; i < ROW_ARITY; i++) {
                res.back().columns[i] = 0;
            }
        }

        return res;
    }
}