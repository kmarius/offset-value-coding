#include "lib/iterators/Filter.h"
#include "lib/iterators/Generator.h"
#include "lib/iterators/Sort.h"
#include "lib/PriorityQueue.h"
#include "lib/log.h"
#include "lib/utils.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/HashDistinct.h"
#include "lib/iterators/AssertEqual.h"
#include "lib/iterators/PrefixTruncationCounter.h"
#include "lib/iterators/Distinct.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/GeneratorZeroPrefix.h"
#include "lib/iterators/GroupBy.h"
#include "lib/iterators/GeneratorZeroSuffix.h"
#include "lib/iterators/Shuffle.h"
#include "lib/iterators/Scan.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/LeftSemiJoin.h"

#include <vector>
#include <iostream>

using namespace ovc;
using namespace ovc::iterators;

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

    ovc::log_info("num_rows=%lu", num_rows);

    auto dedup = new Distinct(
            new Sort(
                    new Generator(num_rows, DOMAIN, 1337)
            ));
    Iterator *plan = new AssertSortedUnique(dedup);
    plan->run();
    ovc_stats &stats = dedup->getInput<Sort>()->getStats();

    ovc::log_info("num_dupex: %d", dedup->num_dupes);


    ovc::log_info("nlogn:                   %lu", (size_t) (num_rows * ::log((double) num_rows)));
    ovc::log_info("comparisons:             %lu", stats.comparisons);
    ovc::log_info("comparisons_of_actual_rows: %lu", stats.comparisons_of_actual_rows);

    delete plan;
}

void example_comparison() {
    size_t num_rows = 1000000;
    size_t seed = 1337;

    auto plan_hash = new HashDistinct(new Generator(num_rows, 100, seed));
    auto plan_sort = new Distinct(new Sort(new Generator(num_rows, 100, seed)));

    hash_set_stats_reset();
    auto start = now();
    plan_hash->run();
    auto duration = since(start);

    auto start_sort = now();
    plan_sort->run();
    auto duration_sort = since(start_sort);

    ovc::log_info("hashing:");
    ovc::log_info("hashes calculated:       %lu", num_rows);
    ovc::log_info("hash_comparisons:        %lu", hs_stats.hash_comparisons);
    ovc::log_info("row_comparisons:         %lu", hs_stats.row_comparisons);
    ovc::log_info("column_comparisons:      %lu", row_equality_column_comparisons);
    ovc::log_info("duration:                %lums", duration);

    ovc_stats &stats = plan_sort->getInput<Sort>()->getStats();

    ovc::log_info("sorting:");
    ovc::log_info("nlogn:                   %lu", num_rows * (size_t) ::log((double) num_rows));
    ovc::log_info("comparisons:             %lu", stats.comparisons);
    ovc::log_info("comparisons_of_actual_rows: %lu", stats.comparisons_of_actual_rows);
    ovc::log_info("column_comparisons:      %lu", stats.column_comparisons);
    ovc::log_info("duration:                %lums", duration_sort);

    delete plan_hash;
    delete plan_sort;
}

void example_sort() {
    // three "memory_runs" are reserved for: low fences, high fences, and the we-are-merging-indicator
    //size_t num_rows = 10000;
    //size_t num_rows = ((1 << RUN_IDX_BITS )- 3) * ((1 << RUN_IDX_BITS) - 3) * QUEUE_CAPACITY;
    size_t num_rows = 2 * ((1 << RUN_IDX_BITS) - 3) * QUEUE_CAPACITY;

    ovc::log_info("num_rows=%lu", num_rows);

    auto plan = new AssertSorted(
            new Sort(
                    new Generator(num_rows, DOMAIN, 1337)
            ));

    plan->run();
    ovc_stats &stats = plan->getInput<Sort>()->getStats();

    ovc::log_info("%d", plan->count());
    ovc::log_info("nlogn:                   %lu", (size_t) (num_rows * ::log((double) num_rows)));
    ovc::log_info("comparisons:             %lu", stats.comparisons);
    ovc::log_info("comparisons_of_actual_rows: %lu", stats.comparisons_of_actual_rows);
    delete plan;
}

void example_hashing() {
    size_t num_rows = 1000000;
    size_t seed = 1337;

    auto plan = new HashDistinct(new Generator(num_rows, 100, seed));

    hash_set_stats_reset();
    auto start = now();
    plan->run();
    auto duration = since(start);

    ovc::log_info("hashing:");
    ovc::log_info("hashes calculated:       %lu", num_rows);
    ovc::log_info("duration:                %lums", duration);
    ovc::log_info("row_comparisons:         %lu", row_num_calls_to_equal);
    ovc::log_info("column_comparisons:      %lu", row_equality_column_comparisons);
    ovc::log_info("calls_to_hash:           %lu", row_num_calls_to_hash);

    delete plan;
}

void example_truncation() {
    size_t num_rows = 1000 * 1000 * 2;
    auto sort = new Sort(new Generator(num_rows, 100, 1337));
    auto plan = new PrefixTruncationCounter(sort);
    plan->run();

    ovc_stats &stats = sort->getStats();
    ovc::log_info("nlogn:                   %lu", (size_t) (num_rows * ::log((double) num_rows)));
    ovc::log_info("comparisons:             %lu", stats.comparisons);
    ovc::log_info("full_comparisons_eq_key: %lu", stats.comparisons_equal_key);
    ovc::log_info("comparisons_of_actual_rows: %lu", stats.comparisons_of_actual_rows);

    ovc::log_info("column comparisons in sort:               %lu", stats.column_comparisons);
    ovc::log_info("column comparisons for prefix truncation: %lu", plan->getColumnComparisons());

    delete plan;
}

void comparison_sort() {
    for (int prefix = 0; prefix < 8; prefix += 2) {
        FILE *file = fopen(("comparison_" + std::to_string(prefix) + ".csv").c_str(), "w");
        fprintf(file, "n,nxk,trunc,sort,sort_no_ovc\n");
        for (size_t i = 1; i <= 10; i++) {
            size_t num_rows = i * 100000;
            auto *gen = new GeneratorZeroPrefix(num_rows, 100, prefix, 1337);
            auto sort = new Sort(gen->clone());
            auto trunc = new PrefixTruncationCounter(sort);

            auto sort2 = new SortNoOvc(gen);

            trunc->run();
            sort2->run();

            fprintf(file, "%lu,%zu,%lu,%lu,%lu\n",
                    num_rows,
                    num_rows * ROW_ARITY,
                    trunc->getColumnComparisons(),
                    sort->getStats().column_comparisons + num_rows,
                    sort2->getStats().column_comparisons);

            delete sort2;
            delete trunc;
        }
        fclose(file);
    }
}

void comparison_dedup() {
    for (int prefix = 0; prefix < 8; prefix += 2) {
        FILE *file = fopen(("column_comparisons_distinct_prefix_" + std::to_string(prefix) + ".csv").c_str(), "w");
        fprintf(file, "n,sort,sort_no_ovc,hash\n");
        for (size_t i = 1; i <= 10; i++) {
            size_t num_rows = i * 100000;

            auto gen = new GeneratorZeroPrefix(num_rows, 100, prefix, 1337);
            auto sort = new SortDistinct(gen->clone());
            auto sort_no_ovc = new SortDistinctNoOvc(gen->clone());
            auto hash = new HashDistinct(gen);

            // reset static counter^
            row_equality_column_comparisons = 0;
            sort->run();
            sort_no_ovc->run();
            hash->run();

            //fprintf(file, "%lu,%zu,%lu,%lu,%lu\n",
            fprintf(file, "%lu,%lu,%lu,%lu\n",
                    num_rows,
                    // num_rows -hash->duplicates,
                    sort->getStats().column_comparisons + num_rows,
                    sort_no_ovc->getStats().column_comparisons,
                    row_equality_column_comparisons);

            delete sort;
            delete sort_no_ovc;
            delete hash;
        }
        fclose(file);
    }
}

void example_count_column_comparisons() {
    log_set_level(LOG_ERROR);
    printf("n,nxk,trunc_comp,sort_comp,sort_no_ovc_comp\n");
    for (size_t i = 1; i < 2; i++) {
        size_t num_rows = i * 100000;
        auto sort = new Sort(new Generator(num_rows, 100, 1337));
        auto trunc = new PrefixTruncationCounter(sort);

        auto sort_no_ovc = new SortNoOvc(new Generator(num_rows, 100, 1337));

        trunc->run();
        sort_no_ovc->run();

        printf("%lu,%zu,%lu,%lu,%lu\n", num_rows, num_rows * ROW_ARITY, trunc->getColumnComparisons(),
               sort->getStats().column_comparisons + num_rows, sort_no_ovc->getStats().column_comparisons);

        delete sort_no_ovc;
        delete trunc;
    }
}

void example_group_by() {
    auto plan = new GroupBy(
            new Sort(new GeneratorZeroSuffix(100000, 128, 6, 1337)),
            1);
    plan->run(true);
    delete plan;
}

void external_shuffle() {
    auto plan = new Shuffle(new Sort(new GeneratorZeroPrefix(4000000, 128)));
    plan->run();
    delete plan;
}

void compare_generate_vs_scan() {
    size_t num_rows = 17000000;
    auto gen = GeneratorZeroPrefix(num_rows, 128, 0, 1337);

    auto *plan_generate = new Sort(gen.clone());
    gen.write("generated.dat");
    auto *plan_scan = new Sort(new Scan("generated.dat"));

    auto start = now();
    plan_generate->run();
    auto d1 = since(start);

    start = now();
    plan_scan->run();
    auto d2 = since(start);

    delete plan_generate;
    delete plan_scan;

    log_info("gen: %lums, scan: %lums", d1, d2);
}

void test_generic_priorityqueue() {
    auto *plan = new SortBase<true, true, RowLessPrefix>(new GeneratorZeroPrefix(100, 10));
    row_less_prefix = 2;
    plan->run(true);
    delete plan;
}

void test_join_tiny() {
    auto *left = new VectorScan({
                                        {0, 0, {1, 2, 7, 0}},
                                        {0, 0, {1, 2, 3, 0}},
                                        {0, 0, {1, 3, 8, 0}},

                                });
    auto *right = new VectorScan({
                                        {0, 0, {1, 2, 0}},
                                        {0, 0, {1, 3, 0}},

                                });
    auto *join = new LeftSemiJoin(left, right, 2);
    join->run(true);
    delete join;
}

void test_join() {
    unsigned upper = 128;
    unsigned join_columns = 4;
    unsigned num_rows = 1000000;

    auto *left = new Sort(new GeneratorZeroPrefix(num_rows, upper));
    auto *right = new Sort(new GeneratorZeroPrefix(num_rows, upper));
    auto *join = new LeftSemiJoin(left, right, join_columns);

    auto expected = (unsigned) (1.0 * num_rows * (1.0 - pow(1 - 1.0 / pow(upper, join_columns), num_rows)));
    log_info("expecting approx. %u results", expected);

    join->run();
    delete join;
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
    //example_hashing();
    //example_truncation();

    //example_count_column_comparisons();

    //comparison_sort();
    //example_group_by();

    //external_shuffle();

    //compare_generate_vs_scan();

    //test_generic_priorityqueue();

    test_join();

    log_info("elapsed=%lums", since(start));

    log_info("fin");
    log_close();
    return 0;
}

