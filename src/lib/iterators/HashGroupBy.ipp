#include <unordered_set>
#include "HashGroupBy.h"
#include "lib/Partitioner.h"
#include "lib/log.h"

namespace ovc::iterators {

    template<typename Aggregate>
    HashGroupBy<Aggregate>::HashGroupBy(Iterator *input, int group_columns, const Aggregate &agg)
            : UnaryIterator(input), group_columns(group_columns), ind(0), agg(agg), count(0) {
        output_is_unique = true;
    }

    template<typename Aggregate>
    void HashGroupBy<Aggregate>::open() {
        Iterator::open();
        input_->open();

        Partitioner partitioner(1 << RUN_IDX_BITS);

        for (Row *row; (row = input_->next()); input_->free()) {
            agg.init(*row);
            row->setHash(group_columns);
            partitioner.put(row);
        }
        partitioner.finalize();
        partitions = partitioner.getPartitions();
        rows = process_partition(partitions.back());
        input_->close();

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
        }

        count++;
        return &rows[ind++];
    }

    template<typename Aggregate>
    std::vector<Row> HashGroupBy<Aggregate>::process_partition(const std::string &path) {

        ExternalRunR part(path, bufferManager, true);

        auto accs = std::unordered_set<Row, std::hash<Row>, RowEqualPrefix>(128, std::hash<Row>(), RowEqualPrefix(group_columns));

        for (Row *row; (row = part.read());) {
            stats.rows_read++;
            auto tup = accs.find(*row);
            if (tup == accs.end()) {
                accs.insert(*row);
            } else {
                // find returns a const reference, but we do not actually change the "key" on aggregate merge
                agg.merge(* (Row *) &*tup, *row);
            }
        }

        part.remove();

        auto res = std::vector<Row>();
        for (auto &it: accs) {
            agg.finalize(* (Row *) &it);
            res.push_back(it);
        }

        return res;
    }
}