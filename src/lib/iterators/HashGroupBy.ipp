#include <unordered_set>
#include "HashGroupBy.h"
#include "lib/Partitioner.h"
#include "lib/log.h"

namespace ovc::iterators {

    template<typename Aggregate>
    HashGroupBy<Aggregate>::HashGroupBy(Iterator *input, int group_columns, const Aggregate &agg)
            : UnaryIterator(input), group_columns(group_columns), ind(0), agg(agg), count(0) {
    }

    template<typename Aggregate>
    void HashGroupBy<Aggregate>::open() {
        Iterator::open();
        input->open();

        Partitioner partitioner(1 << RUN_IDX_BITS);

        for (Row *row; (row = input->next()); input->free()) {
            agg.init(*row);
            row->setHash(group_columns);
            partitioner.put(row);
        }
        partitioner.finalize();
        partitions = partitioner.getPartitionPaths();
        log_info("partitions: %d", partitions.size());
        if (!partitions.empty()) {
            rows = process_partition(partitions.back());
            log_info("rows: %d", rows.size());
        }
        input->close();

        stats.rows_written += partitioner.getStats().rows_written;
    }

    template<typename Aggregate>
    void HashGroupBy<Aggregate>::close() {
        Iterator::close();
        for (auto &path: partitions) {
            ExternalRunR(path, bufferManager, true).remove();
        }
    }

    template<typename Aggregate>
    Row *HashGroupBy<Aggregate>::next() {
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
            log_info("rows: %lu", rows.size());
        }

        count++;
        return &rows[ind++];
    }

    template<typename Aggregate>
    std::vector<Row> HashGroupBy<Aggregate>::process_partition(const std::string &path) {
        log_info("processing partition %s", path.c_str());

        auto eq = RowEqualPrefixNoOVC(group_columns, &stats);
        ExternalRunR part(path, bufferManager, true);
        if (part.definitelyEmpty()) {
            return {};
        }

        Partitioner partitioner(1 << RUN_IDX_BITS);

        for (Row *row; (row = part.read());) {
            log_info("read: %s", row->c_str());
            stats.rows_read++;
            partitioner.putEarlyAggregate(row, agg, eq);
        }

        part.remove();

        auto res = partitioner.finalizeEarlyAggregate(agg);

        std::vector<std::string> new_partitions = partitioner.getPartitionPaths();
        for (auto &p: partitions) {
            new_partitions.push_back(p);
        }
        partitions = new_partitions;
        log_trace("partitions remaining: %lu", partitions.size() - 1);

        return res;
    }
}