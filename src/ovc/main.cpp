#include "lib/utils.h"
#include "lib/iterators/Filter.h"
#include "lib/iterators/IncreasingRangeGenerator.h"
#include "lib/iterators/Sort.h"
#include "lib/PriorityQueue.h"
#include "lib/log.h"
#include "lib/iterators/AssertSortedUnique.h"
#include "lib/iterators/HashDistinct.h"
#include "lib/iterators/PrefixTruncationCounter.h"
#include "lib/iterators/InStreamDistinct.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/ZeroPrefixGenerator.h"
#include "lib/iterators/InStreamGroupBy.h"
#include "lib/iterators/ZeroSuffixGenerator.h"
#include "lib/iterators/Shuffle.h"
#include "lib/iterators/Scan.h"
#include "lib/iterators/VectorScan.h"
#include "lib/iterators/LeftSemiJoin.h"
#include "lib/iterators/LeftSemiHashJoin.h"
#include "lib/iterators/Multiplier.h"
#include "lib/iterators/ApproximateDuplicateGenerator.h"
#include "lib/iterators/InSortGroupBy.h"
#include "lib/iterators/SortDistinct.h"
#include "lib/iterators/HashGroupBy.h"
#include "lib/iterators/DuplicateGenerator.h"
#include "lib/iterators/AssertCorrectOVC.h"
#include "lib/iterators/OVCApplier.h"
#include "lib/iterators/AssertEqual.h"

#include <vector>

using namespace ovc;
using namespace ovc::iterators;

bool pred(const Row *row) {
    return row->columns[0] & 1;
}

void example_simple_tree() {
    Iterator *plan = new Filter(
            new IncreasingRangeGenerator(10000, 1000),
            pred
    );
    plan->run(true);
    delete plan;
}

void example_distinct() {
    size_t num_rows = QUEUE_CAPACITY * QUEUE_CAPACITY * (QUEUE_CAPACITY - 3);

    ovc::log_info("count_=%lu", num_rows);

    auto distinct = new InStreamDistinct(
            new Sort(
                    new IncreasingRangeGenerator(num_rows, DOMAIN, 1337)
            ));
    Iterator *plan = new AssertSortedUnique(distinct);
    plan->run();
    iterator_stats &stats = distinct->getInput<Sort>()->getStats();

    ovc::log_info("num_dupex: %d", distinct->num_dupes);


    ovc::log_info("nlogn:                   %lu", (size_t) (num_rows * ::log((double) num_rows)));
    ovc::log_info("comparisons:             %lu", stats.comparisons);
    ovc::log_info("comparisons_of_actual_rows: %lu", stats.comparisons_of_actual_rows);

    delete plan;
}

void example_comparison() {
    size_t num_rows = 1000000;
    size_t seed = 1337;

    auto plan_hash = new HashDistinct(new IncreasingRangeGenerator(num_rows, 100, seed));
    auto plan_sort = new InStreamDistinct(new Sort(new IncreasingRangeGenerator(num_rows, 100, seed)));

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
    ovc::log_info("column_comparisons:      %lu", plan_hash->getStats().column_comparisons);
    ovc::log_info("duration:                %lums", duration);

    iterator_stats &stats = plan_sort->getInput<Sort>()->getStats();

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
    //size_t count_ = 10000;
    //size_t count_ = ((1 << RUN_IDX_BITS )- 3) * ((1 << RUN_IDX_BITS) - 3) * QUEUE_CAPACITY;
    size_t num_rows = 2 * ((1 << RUN_IDX_BITS) - 3) * QUEUE_CAPACITY;

    ovc::log_info("count_=%lu", num_rows);

    auto plan = new AssertSorted(
            new Sort(
                    new IncreasingRangeGenerator(num_rows, DOMAIN, 1337)
            ));

    plan->run();
    iterator_stats &stats = plan->getInput<Sort>()->getStats();

    ovc::log_info("%d", plan->count());
    ovc::log_info("nlogn:                   %lu", (size_t) (num_rows * ::log((double) num_rows)));
    ovc::log_info("comparisons:             %lu", stats.comparisons);
    ovc::log_info("comparisons_of_actual_rows: %lu", stats.comparisons_of_actual_rows);
    delete plan;
}

void example_hashing() {
    size_t num_rows = 1000000;
    size_t seed = 1337;

    auto plan = new HashDistinct(new IncreasingRangeGenerator(num_rows, 100, seed));

    hash_set_stats_reset();
    auto start = now();
    plan->run();
    auto duration = since(start);

    ovc::log_info("hashing:");
    ovc::log_info("hashes calculated:       %lu", num_rows);
    ovc::log_info("duration:                %lums", duration);
    ovc::log_info("row_comparisons:         %lu", row_num_calls_to_equal);
    ovc::log_info("column_comparisons:      %lu", plan->getStats().column_comparisons);
    ovc::log_info("calls_to_hash:           %lu", row_num_calls_to_hash);

    delete plan;
}

void example_truncation() {
    size_t num_rows = 1000 * 1000 * 2;
    auto sort = new Sort(new IncreasingRangeGenerator(num_rows, 100, 1337));
    auto plan = new PrefixTruncationCounter(sort);
    plan->run();

    iterator_stats &stats = sort->getStats();
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
            auto *gen = new ZeroPrefixGenerator(num_rows, 100, prefix, 1337);
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

void comparison_distinct() {
    for (int prefix = 0; prefix < 8; prefix += 2) {
        FILE *file = fopen(("column_comparisons_distinct_prefix_" + std::to_string(prefix) + ".csv").c_str(), "w");
        fprintf(file, "n,sort,sort_no_ovc,hash\n");
        for (size_t i = 1; i <= 10; i++) {
            size_t num_rows = i * 100000;

            auto gen = new ZeroPrefixGenerator(num_rows, 100, prefix, 1337);
            auto sort = new SortDistinct(gen->clone());
            auto sort_no_ovc = new SortDistinctNoOvc(gen->clone());
            auto hash = new HashDistinct(gen);

            sort->run();
            sort_no_ovc->run();
            hash->run();

            //fprintf(file, "%lu,%zu,%lu,%lu,%lu\n",
            fprintf(file, "%lu,%lu,%lu,%lu\n",
                    num_rows,
                    // count_ -hash->duplicates,
                    sort->getStats().column_comparisons + num_rows,
                    sort_no_ovc->getStats().column_comparisons,
                    hash->getStats().column_comparisons);

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
        auto sort = new Sort(new IncreasingRangeGenerator(num_rows, 100, 1337));
        auto trunc = new PrefixTruncationCounter(sort);

        auto sort_no_ovc = new SortNoOvc(new IncreasingRangeGenerator(num_rows, 100, 1337));

        trunc->run();
        sort_no_ovc->run();

        printf("%lu,%zu,%lu,%lu,%lu\n", num_rows, num_rows * ROW_ARITY, trunc->getColumnComparisons(),
               sort->getStats().column_comparisons + num_rows, sort_no_ovc->getStats().column_comparisons);

        delete sort_no_ovc;
        delete trunc;
    }
}

void example_group_by() {
    auto plan = new InStreamGroupBy(
            new Sort(new ZeroSuffixGenerator(100000, 128, 6, 1337)),
            1, aggregates::Count(1));
    plan->run(true);
    delete plan;
}

void external_shuffle() {
    auto plan = new Shuffle(new Sort(new ZeroPrefixGenerator(4000000, 128)));
    plan->run();
    delete plan;
}

void compare_generate_vs_scan() {
    size_t num_rows = 17000000;
    auto gen = ZeroPrefixGenerator(num_rows, 128, 0, 1337);

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
    RowCmpPrefix cmp(2);
    auto *plan = new SortBase<true, true, RowCmpPrefix>(new ZeroPrefixGenerator(100, 10), cmp);
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

void test_merge_join() {
    unsigned join_columns = 2;

    auto *left = new SortPrefix(new VectorScan({
                                                       {0, 0, {1, 2, 123}},
                                                       {0, 0, {1, 2, 124}},
                                                       {0, 0, {1, 4, 125}},
                                               }), join_columns);
    auto *right = new SortPrefix(new VectorScan({
                                                        {0, 0, {1, 2, 11}},
                                                        {0, 0, {1, 3, 12}},
                                                        {0, 0, {1, 4, 13}},
                                                }), join_columns);
    auto *join = new LeftSemiJoin(left, right, join_columns);

    join->run(true);
    delete join;
}

void test_merge_join2() {
    unsigned upper = 64;
    unsigned join_columns = 2;
    unsigned num_rows_left = 1000;
    unsigned num_rows_right = 10000;

    auto left_rows = SortBase<true, true, RowCmpPrefix>(new ZeroPrefixGenerator(num_rows_left, upper),
                                                        RowCmpPrefix(join_columns)).collect();
    num_rows_left = left_rows.size();

    auto *left = new SortPrefix(new ZeroPrefixGenerator(num_rows_right, upper), join_columns);
    auto *right = new VectorScan(left_rows);
    auto *join = new LeftSemiJoin(left, right, join_columns);

    auto expected = (unsigned) (1.0 * num_rows_left * (1.0 - pow(1 - 1.0 / pow(upper, join_columns), num_rows_right)));
    auto expected2 = (unsigned) (1.0 * num_rows_right * (1.0 - pow(1 - 1.0 / pow(upper, join_columns), num_rows_left)));

    auto joined = join->collect();

    auto joined_distinct = SortBase<true, true, RowCmpPrefix>(new VectorScan(joined),
                                                              RowCmpPrefix(join_columns)).collect();
    log_info("%u rows left", num_rows_left);
    log_info("%u rows after join", joined.size());
    log_info("%u rows distinct after join", joined_distinct.size());
    log_info("%u rows expected", expected);
    log_info("%u rows expected2", expected2);

    //VectorScan(joined_distinct).run(true);

    delete join;
}

void test_merge_join3() {
    unsigned upper = 64;
    unsigned join_columns = 2;
    unsigned num_rows_left = 1000;
    unsigned num_rows_right = 10000;

    auto right_rows = SortBase<true, true, RowCmpPrefix>(new ZeroPrefixGenerator(num_rows_left, upper),
                                                         RowCmpPrefix(join_columns)).collect();
    num_rows_left = right_rows.size();

    auto gen = new ZeroPrefixGenerator(num_rows_right, upper);

    auto *left = new SortPrefix(gen->clone(), join_columns);
    auto *right = new VectorScan(right_rows);
    auto *join = new LeftSemiJoin(left, right, join_columns);

    auto *left2 = new VectorScan(right_rows);
    auto *right2 = new SortPrefix(gen, join_columns);
    auto *join2 = new LeftSemiJoinNoOVC(left2, right2, join_columns);

    join->run();
    join2->run();

    delete join;
}

void test_merge_join4() {
    unsigned upper = 8;
    unsigned join_columns = 4;
    unsigned num_rows_left = 128;
    unsigned num_rows_right = 1024;

    auto right_rows = SortBase<true, true, RowCmpPrefix>(new ZeroPrefixGenerator(num_rows_left, upper),
                                                         RowCmpPrefix(join_columns)).collect();

    auto gen = new ZeroPrefixGenerator(num_rows_right, upper);

    auto *left = new AssertCorrectOVC(new SortPrefix(gen->clone(), join_columns), join_columns);
    auto *right = new AssertCorrectOVC(new VectorScan(right_rows), join_columns);
    auto a = AssertCorrectOVC(new LeftSemiJoin(left, right, join_columns), join_columns);
    a.run(true);
    log_info("%d", a.isCorrect());
    log_info("%lu", a.getInput<LeftSemiJoin>()->getStats().column_comparisons);
}

void compare_joins() {
    int upper = 128;
    int join_columns = 3;
    int num_rows = 1000000;

    auto *left = new ZeroPrefixGenerator(num_rows, upper);
    auto *right = new ZeroPrefixGenerator(num_rows, upper);

    auto *merge_join = new LeftSemiJoin(new Sort(left->clone()), new Sort(right->clone()), join_columns);
    auto *hash_join = new LeftSemiHashJoin(left, right, join_columns);

    auto expected = (unsigned) (1.0 * num_rows * (1.0 - pow(1 - 1.0 / pow(upper, join_columns), num_rows)));
    log_info("expecting approx. %u results", expected);

    auto start = now();
    merge_join->run();
    log_info("merge join: %lums", since(start));
    start = now();

    hash_join->run();
    log_info("hash join: %lums", since(start));

    delete merge_join;
    delete hash_join;
}

void test_compare_prefix() {
    auto *plan = new SortBase<false, true, RowCmpPrefix>(new ZeroPrefixGenerator(10, 128, 1), RowCmpPrefix{2});
    plan->run(true);
    delete plan;
}

#define combine_xor(h1, h2) (h1 ^ h2)
#define combine_complex(h1, h2) (h1 ^ h2 + 0x517cc1b727220a95 + (h1 << 6) + (h1 >> 2))
#define combine_plus(h1, h2) (h1 + h2)
#define combine_java(h1, h2) (31 * h1 + h2)

static inline uint64_t hash(uint64_t val) {
    uint64_t h = 0xcbf29ce484222325;
    for (int i = 0; i < 8; i++) {
        h = (h ^ ((char *) (&val))[i]) * 0x00000100000001b3;
    }
    return h;
}

volatile uint64_t acc = 0;

void hash_combination() {
    Row row = ZeroPrefixGenerator(1, 128).collect()[0];

    const int reps = 10000000;

    auto start = now();
    for (int i = 0; i < reps; i++) {
        uint64_t h = 0;
        for (unsigned long &column: row.columns) {
            unsigned long h1 = hash(column);
            h = combine_xor(h, h1);
        }
        acc ^= h;
    }
    auto time_xor = since(start);

    start = now();
    for (int i = 0; i < reps; i++) {
        uint64_t h = 0;
        for (unsigned long &column: row.columns) {
            unsigned long h1 = hash(column);
            h = combine_complex(h, h1);
        }
        acc ^= h;
    }
    auto time_complex = since(start);

    start = now();
    for (int i = 0; i < reps; i++) {
        uint64_t h = 0;
        for (unsigned long &column: row.columns) {
            unsigned long h1 = hash(column);
            h = combine_plus(h, h1);
        }
        acc ^= h;
    }
    auto time_plus = since(start);

    start = now();
    for (int i = 0; i < reps; i++) {
        uint64_t h = 0;
        for (unsigned long &column: row.columns) {
            unsigned long h1 = hash(column);
            h = combine_java(h, h1);
        }
        acc ^= h;
    }
    auto time_java = since(start);

    log_info("%d repetitions (combining 8 hashes of 8 bytes)", reps);
    log_info("combine_xor: %lums", time_xor);
    log_info("combine_complex: %lums", time_complex);
    log_info("combine_plus: %lums", time_plus);
    log_info("combine_java: %lums", time_java);
}

void example_duplicate_generation() {
    int prefix = 4;
    int num_unique = 4;
    int mult = 4;
    auto *shuf = new Shuffle(new Multiplier(new ZeroPrefixGenerator(num_unique, 1024, prefix), mult));
    auto *plan = new SortDistinct(shuf);
    plan->run(true);
    log_info("%lu", shuf->getCount());
    delete plan;
}

void experiment_sort_distinct() {
    int num_rows = 1000000;
    int num_experiments = 4;
    log_set_quiet(true);
    printf("input_size,output_size,column_comparisons_ovc,column_comparisons_no_ovc\n");
    for (int i = 0; i < num_experiments; i++) {
        auto unique = (i + 1) * (1.0 / num_experiments);
        auto *gen = new ApproximateDuplicateGenerator(num_rows, unique, 0, 0, 1337);

        auto *plan_ovc = new SortDistinct(gen->clone());
        auto *plan_no_ovc = new SortDistinctNoOvc(gen->clone());

        plan_ovc->run();
        plan_no_ovc->run();

        printf("%d,%lu,%lu,%lu\n", num_rows, plan_ovc->getCount(), plan_ovc->getStats().column_comparisons,
               plan_no_ovc->getStats().column_comparisons);

        delete plan_ovc;
        delete plan_no_ovc;
        delete gen;
    }

    log_set_quiet(false);
}

void example_in_sort_group_by() {
    auto *plan = new InSortGroupBy(
            new ZeroSuffixGenerator(10000, 16, 0),
            2,
            aggregates::Avg(2, 2)
    );
    plan->run(true);
    delete plan;

    auto *plan2 = new InSortGroupBy(
            new ZeroSuffixGenerator(10000, 16, 0),
            2,
            aggregates::Sum(2, 2)
    );
    plan2->run(true);
    delete plan2;
}

void example_in_sort_group_by_no_ovc() {
    auto *plan = new InSortGroupByNoOvc(
            new ZeroSuffixGenerator(20, 8, 0),
            1,
            aggregates::Count(1)
    );
    plan->run(true);
    delete plan;
}

void experiment_group_by() {
    int num_rows = 16000000;
    int num_experiments = 4;

    log_set_quiet(true);
    printf("experiment,input_size,output_size,column_comparisons,rows_written\n");
    for (int i = 0; i < num_experiments; i++) {
        int zero_prefix = 0;
        int group_columns = 4;

        auto agg = aggregates::Avg(group_columns, group_columns);

        auto unique = (i + 1) * (1.0 / num_experiments);
        auto *gen = new ApproximateDuplicateGenerator(num_rows, unique, zero_prefix, ROW_ARITY - group_columns, 1337);

        auto *hash = new HashGroupBy(gen->clone(), group_columns, agg);

        auto *in_stream_scan = new InStreamGroupByNoOvc(
                new Sort(gen->clone()),
                group_columns,
                agg);

        // we only sort by the group-by columns here
        auto *in_stream_sort_ovc = new InStreamGroupBy(
                new SortBase<false, true, RowCmpPrefix>(gen->clone(), RowCmpPrefix(group_columns)),
                group_columns,
                agg);

        auto *in_stream_sort_no_ovc = new InStreamGroupByNoOvc(
                new SortBase<false, false, RowCmpPrefix>(gen->clone(), RowCmpPrefix(group_columns)),
                group_columns,
                agg);

        auto *in_sort_ovc = new InSortGroupBy(
                gen->clone(),
                group_columns,
                agg);

        auto *in_sort_no_ovc = new InSortGroupByNoOvc(
                gen->clone(),
                group_columns,
                agg);

        hash->run();
        printf("%s,%d,%lu,%lu,%lu\n", "hash", num_rows, hash->getCount(), 0L, 0L);

        in_stream_scan->run();
        printf("%s,%d,%lu,%lu,%lu\n", "in_stream_scan", num_rows, in_stream_scan->getCount(),
               in_stream_scan->getStats().column_comparisons, in_stream_scan->getStats().rows_written);

        in_sort_ovc->run();
        printf("%s,%d,%lu,%lu,%lu\n", "in_sort_ovc", num_rows, in_sort_ovc->getCount(),
               in_sort_ovc->getStats().column_comparisons, in_sort_ovc->getStats().rows_written);

        in_stream_sort_ovc->run();
        printf("%s,%d,%lu,%lu,%lu\n", "in_stream_sort_ovc", num_rows, in_stream_sort_ovc->getCount(),
               in_stream_sort_ovc->getStats().column_comparisons +
               in_stream_sort_ovc->getInput<SortBase<false, true, RowCmpPrefix>>()->getStats().column_comparisons,
               in_stream_sort_ovc->getStats().rows_written);

        in_sort_no_ovc->run();
        printf("%s,%d,%lu,%lu,%lu\n", "in_sort_no_ovc", num_rows, in_sort_no_ovc->getCount(),
               in_sort_no_ovc->getStats().column_comparisons, in_sort_no_ovc->getStats().rows_written);

        in_stream_sort_no_ovc->run();
        printf("%s,%d,%lu,%lu,%lu\n", "in_stream_sort_no_ovc", num_rows, in_stream_sort_no_ovc->getCount(),
               in_stream_sort_no_ovc->getStats().column_comparisons +
               in_stream_sort_no_ovc->getInput<SortBase<false, false, RowCmpPrefix>>()->getStats().column_comparisons,
               in_stream_sort_no_ovc->getInput<SortBase<false, false, RowCmpPrefix>>()->getStats().rows_written);

        delete hash;
        delete in_sort_ovc;
        delete in_sort_no_ovc;
        delete in_stream_scan;
        delete in_stream_sort_ovc;
        delete in_stream_sort_no_ovc;
        delete gen;
    }

    log_set_quiet(false);
}

void experiment_group_by2() {
    int num_rows = 1000000;
    int num_experiments = 10;
    int zero_prefix = 0;
    int group_columns = 6;

    log_set_quiet(true);
    const char *results_path = "results.csv";
    FILE *results = fopen(results_path, "w");

    fprintf(results, "experiment,group_columns,zero_prefix,input_size,output_size,column_comparisons,rows_written\n");
    for (int i = 0; i < num_experiments; i++) {

        auto agg = aggregates::Avg(group_columns, group_columns);

        auto unique = (i + 1) * (1.0 / num_experiments);
        auto *gen = new ApproximateDuplicateGenerator(num_rows, unique, zero_prefix, ROW_ARITY - group_columns, 1337);

        auto *hash = new HashGroupBy(gen->clone(), group_columns, agg);

        auto *in_stream_scan = new InStreamGroupByNoOvc(
                new Sort(gen->clone()),
                group_columns,
                agg);

        // we only sort by the group-by columns here
        auto *in_stream_sort_ovc = new InStreamGroupBy(
                new SortBase<false, true, RowCmpPrefix>(gen->clone(), RowCmpPrefix(group_columns)),
                group_columns,
                agg);

        auto *in_stream_sort_no_ovc = new InStreamGroupByNoOvc(
                new SortBase<false, false, RowCmpPrefix>(gen->clone(), RowCmpPrefix(group_columns)),
                group_columns,
                agg);

        auto *in_sort_ovc = new InSortGroupBy(
                gen->clone(),
                group_columns,
                agg);

        auto *in_sort_no_ovc = new InSortGroupByNoOvc(
                gen->clone(),
                group_columns,
                agg);

        hash->run();
        fprintf(results, "%s,%d,%d,%d,%lu,%lu,%d\n", "hash", group_columns, zero_prefix, num_rows, hash->getCount(),
                hash->getStats().column_comparisons, num_rows);

        in_stream_scan->run();
        fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu\n", "in_stream_scan", group_columns, zero_prefix, num_rows,
                in_stream_scan->getCount(),
                in_stream_scan->getStats().column_comparisons, in_stream_scan->getStats().rows_written);

        in_sort_ovc->run();
        fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu\n", "in_sort_ovc", group_columns, zero_prefix, num_rows,
                in_sort_ovc->getCount(),
                in_sort_ovc->getStats().column_comparisons + num_rows, in_sort_ovc->getStats().rows_written);

        in_stream_sort_ovc->run();
        fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu\n", "in_stream_sort_ovc", group_columns, zero_prefix, num_rows,
                in_stream_sort_ovc->getCount(),
                in_stream_sort_ovc->getStats().column_comparisons +
                in_stream_sort_ovc->getInput<SortBase<false, true, RowCmpPrefix>>()->getStats().column_comparisons +
                num_rows,
                in_stream_sort_ovc->getInput<SortBase<false, true, RowCmpPrefix>>()->getStats().rows_written);

        in_sort_no_ovc->run();
        fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu\n", "in_sort_no_ovc", group_columns, zero_prefix, num_rows,
                in_sort_no_ovc->getCount(),
                in_sort_no_ovc->getStats().column_comparisons, in_sort_no_ovc->getStats().rows_written);

        in_stream_sort_no_ovc->run();
        fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu\n", "in_stream_sort_no_ovc", group_columns, zero_prefix, num_rows,
                in_stream_sort_no_ovc->getCount(),
                in_stream_sort_no_ovc->getStats().column_comparisons +
                in_stream_sort_no_ovc->getInput<SortBase<false, false, RowCmpPrefix>>()->getStats().column_comparisons,
                in_stream_sort_no_ovc->getInput<SortBase<false, false, RowCmpPrefix>>()->getStats().rows_written);

        delete hash;
        delete in_sort_ovc;
        delete in_sort_no_ovc;
        delete in_stream_scan;
        delete in_stream_sort_ovc;
        delete in_stream_sort_no_ovc;
        delete gen;
    }

    fclose(results);
    log_set_quiet(false);
}

void example_stats() {
    auto *plan = new InSortGroupBy(
            new ApproximateDuplicateGenerator(1000000, 0.5, 0, ROW_ARITY - 2, 1337),
            2,
            aggregates::Avg(2, 2));
    plan->run();
    log_info("%lu, %lu", plan->getStats().rows_written, plan->getStats().rows_read);
    delete plan;
}

void experiment_group_by3() {
    int num_rows = 1000000;
    int num_experiments = 10;

    log_set_quiet(true);

    const char *results_path = "results.csv";
    FILE *results = fopen(results_path, "w");
    fprintf(results, "experiment,group_columns,zero_prefix,input_size,output_size,column_comparisons,rows_written\n");

    for (int group_columns = 0; group_columns < ROW_ARITY - 1; group_columns++) {
        for (int zero_prefix = 0; zero_prefix < group_columns; zero_prefix++) {
            for (int i = 0; i < num_experiments; i++) {

                auto agg = aggregates::Avg(group_columns, group_columns);

                auto unique = (i + 1) * (1.0 / num_experiments);
                auto gen = ApproximateDuplicateGenerator(num_rows, unique, zero_prefix, ROW_ARITY - group_columns,
                                                         1337);

                auto hash = HashGroupBy(gen.clone(), group_columns, agg);

                auto in_stream_scan = InStreamGroupByNoOvc(
                        new Sort(gen.clone()),
                        group_columns,
                        agg);

                // we only sort by the group-by columns here
                auto in_stream_sort_ovc = InStreamGroupBy(
                        new SortPrefix(gen.clone(), group_columns),
                        group_columns,
                        agg);

                auto in_stream_sort_no_ovc = InStreamGroupByNoOvc(
                        new SortPrefixNoOvc(gen.clone(), group_columns),
                        group_columns,
                        agg);

                auto in_sort_ovc = InSortGroupBy(
                        gen.clone(),
                        group_columns,
                        agg);

                auto in_sort_no_ovc = InSortGroupByNoOvc(
                        gen.clone(),
                        group_columns,
                        agg);

                hash.run();
                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%d\n", "hash", group_columns, zero_prefix, num_rows,
                        hash.getCount(), hash.getStats().column_comparisons, num_rows);

                in_stream_scan.run();
                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu\n", "in_stream_scan", group_columns, zero_prefix, num_rows,
                        in_stream_scan.getCount(),
                        in_stream_scan.getStats().column_comparisons, in_stream_scan.getStats().rows_written);

                in_sort_ovc.run();
                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu\n", "in_sort_ovc", group_columns, zero_prefix, num_rows,
                        in_sort_ovc.getCount(),
                        in_sort_ovc.getStats().column_comparisons + num_rows, in_sort_ovc.getStats().rows_written);

                in_stream_sort_ovc.run();
                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu\n", "in_stream_sort_ovc", group_columns, zero_prefix,
                        num_rows, in_stream_sort_ovc.getCount(),
                        in_stream_sort_ovc.getStats().column_comparisons +
                        in_stream_sort_ovc.getInput<SortPrefix>()->getStats().column_comparisons +
                        num_rows,
                        in_stream_sort_ovc.getInput<SortPrefix>()->getStats().rows_written);

                in_sort_no_ovc.run();
                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu\n", "in_sort_no_ovc", group_columns, zero_prefix, num_rows,
                        in_sort_no_ovc.getCount(),
                        in_sort_no_ovc.getStats().column_comparisons, in_sort_no_ovc.getStats().rows_written);

                in_stream_sort_no_ovc.run();
                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu\n", "in_stream_sort_no_ovc", group_columns, zero_prefix,
                        num_rows, in_stream_sort_no_ovc.getCount(),
                        in_stream_sort_no_ovc.getStats().column_comparisons +
                        in_stream_sort_no_ovc.getInput<SortPrefixNoOvc>()->getStats().column_comparisons,
                        in_stream_sort_no_ovc.getInput<SortPrefixNoOvc>()->getStats().rows_written);
            }
        }
    }

    fclose(results);
    log_set_quiet(false);
}

void experiment_group_by4() {
    int num_rows = 1 << 20;
    int num_experiments = 11;

    log_set_quiet(true);

    const char *results_path = "results5_1m_4k.csv";
    FILE *results = fopen(results_path, "w");
    fprintf(results,
            "experiment,group_columns,zero_prefix,input_size,output_size,column_comparisons,rows_written,duration\n");

    for (int group_columns = 0; group_columns < ROW_ARITY - 1; group_columns++) {
        for (int zero_prefix = 0; zero_prefix < group_columns - 1; zero_prefix++) {
            for (int i = 0; i < num_experiments; i++) {
                log_set_quiet(false);
                log_info("group_by with num_rows=%lu, group_columns=%d, zero_prefix=%d output_size=%lu",
                         num_rows, group_columns, zero_prefix, num_rows >> i);
                log_set_quiet(true);

                auto agg = aggregates::Avg(group_columns, group_columns);

                auto gen = DuplicateGenerator(num_rows, 1 << i, zero_prefix, 1337);

                auto hash = HashGroupBy(gen.clone(), group_columns, agg);

                auto in_stream_scan = InStreamGroupByNoOvc(new Sort(gen.clone()), group_columns, agg);

                // we only sort by the group-by columns here
                auto in_stream_sort_ovc = InStreamGroupBy(
                        new SortPrefix(gen.clone(), group_columns),
                        group_columns,
                        agg);

                auto in_stream_sort_no_ovc = InStreamGroupByNoOvc(
                        new SortPrefixNoOvc(gen.clone(), group_columns),
                        group_columns,
                        agg);

                auto in_sort_ovc = InSortGroupBy(gen.clone(), group_columns, agg);

                auto in_sort_no_ovc = InSortGroupByNoOvc(gen.clone(), group_columns, agg);


                auto t0 = now();
                hash.run();
                auto hash_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%zu,%lu\n", "hash", group_columns, zero_prefix, num_rows,
                        hash.getCount(), hash.getStats().column_comparisons, hash.getStats().rows_written,
                        hash_duration);
                fflush(results);

                t0 = now();
                in_stream_scan.run();
                auto in_stream_scan_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_stream_scan", group_columns, zero_prefix,
                        num_rows,
                        in_stream_scan.getCount(),
                        in_stream_scan.getStats().column_comparisons, in_stream_scan.getStats().rows_written,
                        in_stream_scan_duration);

                t0 = now();
                in_sort_ovc.run();
                auto in_sort_ovc_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_sort_ovc", group_columns, zero_prefix, num_rows,
                        in_sort_ovc.getCount(),
                        in_sort_ovc.getStats().column_comparisons + num_rows, in_sort_ovc.getStats().rows_written,
                        in_sort_ovc_duration);

                t0 = now();
                in_stream_sort_ovc.run();
                auto in_stream_sort_ovc_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_stream_sort_ovc", group_columns, zero_prefix,
                        num_rows, in_stream_sort_ovc.getCount(),
                        in_stream_sort_ovc.getStats().column_comparisons +
                        in_stream_sort_ovc.getInput<SortPrefix>()->getStats().column_comparisons +
                        num_rows,
                        in_stream_sort_ovc.getInput<SortPrefix>()->getStats().rows_written,
                        in_stream_sort_ovc_duration);

                t0 = now();
                in_sort_no_ovc.run();
                auto in_sort_no_ovc_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_sort_no_ovc", group_columns, zero_prefix,
                        num_rows,
                        in_sort_no_ovc.getCount(),
                        in_sort_no_ovc.getStats().column_comparisons, in_sort_no_ovc.getStats().rows_written,
                        in_sort_no_ovc_duration);

                t0 = now();
                in_stream_sort_no_ovc.run();
                auto in_stream_sort_no_ovc_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_stream_sort_no_ovc", group_columns, zero_prefix,
                        num_rows, in_stream_sort_no_ovc.getCount(),
                        in_stream_sort_no_ovc.getStats().column_comparisons +
                        in_stream_sort_no_ovc.getInput<SortPrefixNoOvc>()->getStats().column_comparisons,
                        in_stream_sort_no_ovc.getInput<SortPrefixNoOvc>()->getStats().rows_written,
                        in_stream_sort_no_ovc_duration);

            }
        }
    }

    fclose(results);
    log_set_quiet(false);
}

void experiment_group_by5() {
    int num_rows = 1 << 20;
    int num_experiments = 8;

    log_set_quiet(true);

    const char *results_path = "results_1m_4k.csv";
    FILE *results = fopen(results_path, "w");
    fprintf(results,
            "experiment,group_columns,zero_prefix,input_size,output_size,column_comparisons,rows_written,duration\n");

    for (int group_columns = 0; group_columns < ROW_ARITY - 1; group_columns++) {
        for (int zero_prefix = 0; zero_prefix < group_columns - 1; zero_prefix++) {
            for (int i = 0; i < num_experiments; i++) {
                log_set_quiet(false);
                log_info("group_by with num_rows=%lu, group_columns=%d, zero_prefix=%d output_size=%lu",
                         num_rows, group_columns, zero_prefix, num_rows >> i);
                log_set_quiet(true);

                auto agg = aggregates::Avg(group_columns, group_columns);

                auto unique = (i + 1) * (1.0 / num_experiments);
                auto gen = ApproximateDuplicateGenerator(num_rows, unique, zero_prefix, ROW_ARITY - group_columns,
                                                         1337);

                auto hash = HashGroupBy(gen.clone(), group_columns, agg);

                auto in_stream_scan = InStreamGroupByNoOvc(new Sort(gen.clone()), group_columns, agg);

                // we only sort by the group-by columns here
                auto in_stream_sort_ovc = InStreamGroupBy(
                        new SortPrefix(gen.clone(), group_columns),
                        group_columns,
                        agg);

                auto in_stream_sort_no_ovc = InStreamGroupByNoOvc(
                        new SortPrefixNoOvc(gen.clone(), group_columns),
                        group_columns,
                        agg);

                auto in_sort_ovc = InSortGroupBy(gen.clone(), group_columns, agg);

                auto in_sort_no_ovc = InSortGroupByNoOvc(gen.clone(), group_columns, agg);


                auto t0 = now();
                hash.run();
                auto hash_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%zu,%lu\n", "hash", group_columns, zero_prefix, num_rows,
                        hash.getCount(), hash.getStats().column_comparisons, hash.getStats().rows_written,
                        hash_duration);
                fflush(results);

                t0 = now();
                in_stream_scan.run();
                auto in_stream_scan_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_stream_scan", group_columns, zero_prefix,
                        num_rows,
                        in_stream_scan.getCount(),
                        in_stream_scan.getStats().column_comparisons, in_stream_scan.getStats().rows_written,
                        in_stream_scan_duration);

                t0 = now();
                in_sort_ovc.run();
                auto in_sort_ovc_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_sort_ovc", group_columns, zero_prefix, num_rows,
                        in_sort_ovc.getCount(),
                        in_sort_ovc.getStats().column_comparisons + num_rows, in_sort_ovc.getStats().rows_written,
                        in_sort_ovc_duration);

                t0 = now();
                in_stream_sort_ovc.run();
                auto in_stream_sort_ovc_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_stream_sort_ovc", group_columns, zero_prefix,
                        num_rows, in_stream_sort_ovc.getCount(),
                        in_stream_sort_ovc.getStats().column_comparisons +
                        in_stream_sort_ovc.getInput<SortPrefix>()->getStats().column_comparisons +
                        num_rows,
                        in_stream_sort_ovc.getInput<SortPrefix>()->getStats().rows_written,
                        in_stream_sort_ovc_duration);

                t0 = now();
                in_sort_no_ovc.run();
                auto in_sort_no_ovc_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_sort_no_ovc", group_columns, zero_prefix,
                        num_rows,
                        in_sort_no_ovc.getCount(),
                        in_sort_no_ovc.getStats().column_comparisons, in_sort_no_ovc.getStats().rows_written,
                        in_sort_no_ovc_duration);

                t0 = now();
                in_stream_sort_no_ovc.run();
                auto in_stream_sort_no_ovc_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_stream_sort_no_ovc", group_columns, zero_prefix,
                        num_rows, in_stream_sort_no_ovc.getCount(),
                        in_stream_sort_no_ovc.getStats().column_comparisons +
                        in_stream_sort_no_ovc.getInput<SortPrefixNoOvc>()->getStats().column_comparisons,
                        in_stream_sort_no_ovc.getInput<SortPrefixNoOvc>()->getStats().rows_written,
                        in_stream_sort_no_ovc_duration);

            }
        }
    }

    fclose(results);
    log_set_quiet(false);
}

void test_hash_agg() {
    auto plan = new HashGroupBy(new ZeroPrefixGenerator(4, 100, 0),
                                2, aggregates::Count(2));
    plan->run(true);
    log_info("%lu", plan->getStats().column_comparisons);
    delete plan;
}

void experiment_joins1() {
    int num_rows = 1 << 20;
    int num_experiments = 1;

    log_set_quiet(false);

    const char *results_path = "joins1.csv";
    //FILE *results = fopen(results_path, "w");
    FILE *results = stdout;
    fprintf(results,
            "experiment,group_columns,zero_prefix,input_size,output_size,column_comparisons,rows_written,duration\n");

    for (int join_columns = 4; join_columns < 5; join_columns++) {
        for (int zero_prefix = 0; zero_prefix < 1; zero_prefix++) {
            for (int i = 0; i < num_experiments; i++) {
                log_set_quiet(false);
                log_info("join with num_rows=%lu, join_columns=%d, zero_prefix=%d output_size=%lu",
                         num_rows, join_columns, zero_prefix, num_rows >> i);
                //log_set_quiet(true);

                auto gen1 = ZeroPrefixGenerator(num_rows, 32, 0, 1337);
                auto gen2 = ZeroPrefixGenerator(num_rows, 32, 0, 1337 + 1);
                auto gen3 = ZeroPrefixGenerator(num_rows, 32, 0, 1337 + 2);

                LeftSemiJoin(new SortPrefix(gen1.clone(), join_columns),
                             new SortPrefix(gen2.clone(), join_columns),
                             join_columns).run();

                auto plan1 = LeftSemiJoin(
                        new LeftSemiJoin(new SortPrefix(gen1.clone(), join_columns),
                                         new SortPrefix(gen2.clone(), join_columns),
                                         join_columns),
                        new SortPrefix(gen3.clone(), join_columns),
                        join_columns
                );

                auto plan2 = LeftSemiJoin(
                        new OVCApplier(
                                new LeftSemiJoin(
                                        new SortPrefix(gen1.clone(), join_columns),
                                        new SortPrefix(gen2.clone(), join_columns),
                                        join_columns),
                                join_columns),
                        new SortPrefix(gen3.clone(), join_columns),
                        join_columns
                );

                auto plan3 = LeftSemiJoinNoOVC(
                        new OVCApplier(
                                new LeftSemiJoin(
                                        new SortPrefix(gen1.clone(), join_columns),
                                        new SortPrefix(gen2.clone(), join_columns),
                                        join_columns),
                                join_columns),
                        new SortPrefix(gen3.clone(), join_columns),
                        join_columns
                );

                auto plan4 = LeftSemiJoinNoOVC(
                        new LeftSemiJoinNoOVC(
                                new SortPrefixNoOvc(gen1.clone(), join_columns),
                                new SortPrefixNoOvc(gen2.clone(), join_columns),
                                join_columns),
                        new SortPrefixNoOvc(gen3.clone(), join_columns),
                        join_columns
                );

                {
                    auto t0 = now();
                    plan1.run();
                    auto plan1_duration = since(t0);

                    struct iterator_stats stats = {0};
                    plan1.accumulateStats(stats);

                    fprintf(results, "%s,%d,%d,%d,%d,%lu,%zu,%lu\n", "plan1", join_columns, zero_prefix, num_rows,
                            0, stats.column_comparisons,
                            stats.rows_written,
                            plan1_duration);
                    fflush(results);
                }

                {
                    auto t0 = now();
                    plan2.run();
                    auto plan2_duration = since(t0);

                    struct iterator_stats stats = {0};
                    plan2.accumulateStats(stats);

                    fprintf(results, "%s,%d,%d,%d,%d,%lu,%zu,%lu\n", "plan2", join_columns, zero_prefix, num_rows,
                            0, stats.column_comparisons,
                            stats.rows_written,
                            plan2_duration);
                    fflush(results);
                }

                {
                    auto t0 = now();
                    plan3.run();
                    auto plan3_duration = since(t0);

                    struct iterator_stats stats = {0};
                    plan3.accumulateStats(stats);

                    fprintf(results, "%s,%d,%d,%d,%d,%lu,%zu,%lu\n", "plan3", join_columns, zero_prefix, num_rows,
                            0, stats.column_comparisons,
                            stats.rows_written,
                            plan3_duration);
                    fflush(results);
                }

                {
                    auto t0 = now();
                    plan4.run();
                    auto plan4_duration = since(t0);

                    struct iterator_stats stats = {0};
                    plan4.accumulateStats(stats);

                    fprintf(results, "%s,%d,%d,%d,%d,%lu,%zu,%lu\n", "plan4", join_columns, zero_prefix, num_rows,
                            0, stats.column_comparisons,
                            stats.rows_written,
                            plan4_duration);
                    fflush(results);
                }
            }
        }
    }

    //fclose(results);
    log_set_quiet(false);
}

int main(int argc, char *argv[]) {
    log_open(LOG_TRACE);
    log_set_quiet(false);
    log_set_level(LOG_INFO);
    log_info("start", "");

    auto start = now();

    //raw_rows();
    //example_sort();
    //example_distinct();
    //example_comparison();
    //example_hashing();
    //example_truncation();

    //example_count_column_comparisons();

    //comparison_sort();
    //example_group_by();

    //external_shuffle();

    //compare_generate_vs_scan();

    //test_generic_priorityqueue();

    //test_merge_join();
    //test_merge_join2();
    //test_merge_join3();
    //test_merge_join4();
    //compare_joins();

    //test_compare_prefix();

    // hash_combination();

    // example_duplicate_generation();
    //experiment_sort_distinct();

    //example_in_sort_group_by();
    //example_in_sort_group_by_no_ovc();

    //experiment_group_by();
    //experiment_group_by2();
    //experiment_group_by3();
    //experiment_group_by4();
    //experiment_group_by5();

    //example_stats();

    //test_hash_agg();

    //experiment_joins1();

    log_info("elapsed=%lums", since(start));

    log_info("fin");
    log_close();
    return 0;
}

