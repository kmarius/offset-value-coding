#include "lib/iterators/Filter.h"
#include "lib/iterators/Generator.h"
#include "lib/iterators/Sort.h"
#include "lib/PriorityQueue.h"
#include "lib/log.h"
#include "lib/utils.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/HashDedup.h"
#include "lib/iterators/AssertEqual.h"
#include "lib/iterators/PrefixTruncationCounter.h"
#include "lib/iterators/Dedup.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/GeneratorZeroPrefix.h"

#include <vector>

bool pred(const Row *row) {
    return row->columns[0] & 1;
}

void example_simple_tree() {
    Iterator *plan = new Filter(
            new Generator(10000, 1000),
            pred
    );
    plan->run(true);
    delete plan;
}

void example_dedup() {
    size_t num_rows = QUEUE_CAPACITY * QUEUE_CAPACITY * (QUEUE_CAPACITY - 3);

    log_info("num_rows=%lu", num_rows);

    auto dedup = new Dedup(
            new Sort(
                    new Generator(num_rows, DOMAIN, 1337)
            ));
    Iterator *plan = new AssertSortedUnique(dedup);
    plan->run();

    log_info("num_dupex: %d", dedup->num_dupes);

    delete plan;

    log_info("nlogn:                   %lu", (size_t) (num_rows * log((double) num_rows)));
    log_info("comparisons:             %lu", stats.comparisons);
    log_info("full_comparisons:        %lu", stats.full_comparisons);
    log_info("actual_full_comparisons: %lu", stats.actual_full_comparisons);
}

void example_comparison() {
    size_t num_rows = 1000000;
    size_t seed = 1337;

    auto plan_hash = new HashDedup(new Generator(num_rows, 100, seed));
    auto plan_sort = new Dedup(new Sort(new Generator(num_rows, 100, seed)));

    hash_set_stats_reset();
    auto start = now();
    plan_hash->run();
    auto duration = since(start);

    priority_queue_stats_reset();
    auto start_sort = now();
    plan_sort->run();
    auto duration_sort = since(start_sort);

    log_info("hashing:");
    log_info("hashes calculated:       %lu", num_rows);
    log_info("hash_comparisons:        %lu", hs_stats.hash_comparisons);
    log_info("row_comparisons:         %lu", hs_stats.row_comparisons);
    log_info("column_comparisons:      %lu", row_equality_column_comparisons);
    log_info("duration:                %lums", duration);

    log_info("sorting:");
    log_info("nlogn:                   %lu", num_rows * (size_t) log((double) num_rows));
    log_info("comparisons:             %lu", stats.comparisons);
    log_info("full_comparisons:        %lu", stats.full_comparisons);
    log_info("actual_full_comparisons: %lu", stats.actual_full_comparisons);
    log_info("column_comparisons:      %lu", plan_sort->getInput<Sort>()->getColumnComparisons());
    log_info("duration:                %lums", duration_sort);

    delete plan_hash;
    delete plan_sort;
}

void example_sort() {
    // three "memory_runs" are reserved for: low fences, high fences, and the we-are-merging-indicator
    //size_t num_rows = 10000;
    //size_t num_rows = ((1 << RUN_IDX_BITS )- 3) * ((1 << RUN_IDX_BITS) - 3) * QUEUE_CAPACITY;
    size_t num_rows = 2 * ((1 << RUN_IDX_BITS) - 3) * QUEUE_CAPACITY;

    log_info("num_rows=%lu", num_rows);

    auto plan = new AssertSorted(
            new Sort(
                    new Generator(num_rows, DOMAIN, 1337)
            ));

    plan->run();

    log_info("%d", plan->count());
    log_info("nlogn:                   %lu", (size_t) (num_rows * log((double) num_rows)));
    log_info("comparisons:             %lu", stats.comparisons);
    log_info("full_comparisons:        %lu", stats.full_comparisons);
    log_info("actual_full_comparisons: %lu", stats.actual_full_comparisons);
    delete plan;
}

void example_hashing() {
    size_t num_rows = 1000000;
    size_t seed = 1337;

    auto plan = new HashDedup(new Generator(num_rows, 100, seed));

    hash_set_stats_reset();
    auto start = now();
    plan->run();
    auto duration = since(start);

    log_info("hashing:");
    log_info("hashes calculated:       %lu", num_rows);
    log_info("duration:                %lums", duration);
    log_info("row_comparisons:         %lu", row_num_calls_to_equal);
    log_info("column_comparisons:      %lu", row_equality_column_comparisons);
    log_info("calls_to_hash:           %lu", row_num_calls_to_hash);

    delete plan;
}

void example_truncation() {
    size_t num_rows = 1000 * 1000 * 2;
    auto sort = new Sort(new Generator(num_rows, 100, 1337));
    auto plan = new PrefixTruncationCounter(sort);
    plan->run();

    log_info("nlogn:                   %lu", (size_t) (num_rows * log((double) num_rows)));
    log_info("comparisons:             %lu", stats.comparisons);
    log_info("full_comparisons:        %lu", stats.full_comparisons);
    log_info("full_comparisons_eq_key: %lu", stats.full_comparisons_equal_key);
    log_info("actual_full_comparisons: %lu", stats.actual_full_comparisons);

    log_info("column comparisons in sort:               %lu", sort->getColumnComparisons());
    log_info("column comparisons for prefix truncation: %lu", plan->getColumnComparisons());

    delete plan;
}

void example_count_column_comparisons() {
    log_set_level(LOG_ERROR);
    printf("n,nxk,trunc_comp,sort_comp\n");
    for (size_t num_rows: {100000, 500000, 2000000}) {
        auto sort = new Sort(new Generator(num_rows, 100, 1337));
        auto plan = new PrefixTruncationCounter(sort);
        plan->run();

        printf("%lu,%zu,%lu,%lu\n", num_rows, num_rows * ROW_ARITY, plan->getColumnComparisons(), sort->getColumnComparisons());

        log_info("nlogn:                   %lu", (size_t) (num_rows * log((double) num_rows)));
        log_info("comparisons:             %lu", stats.comparisons);
        log_info("full_comparisons:        %lu", stats.full_comparisons);
        delete plan;
    }
}

int main(int argc, char *argv[]) {
    log_open(LOG_TRACE);
    log_set_quiet(false);
    log_set_level(LOG_INFO);
    log_info("start", "");

    auto start = now();

    //raw_rows();
    //example_sort();
    //example_dedup();
    //example_comparison();
    example_hashing();
    //example_truncation();

    example_count_column_comparisons();

    log_info("elapsed=%lums", since(start));

    log_info("fin");
    log_close();
    return 0;
}