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
#include "lib/iterators/RowGenerator.h"
#include "lib/iterators/InStreamGroupBy.h"
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
#include "lib/iterators/Transposer.h"
#include "lib/iterators/RowGeneratorWithDomains.h"
#include "lib/iterators/UniqueRowGenerator.h"
#include "lib/iterators/SegmentedSort.h"

#include <vector>

using namespace ovc;
using namespace ovc::iterators;

void experiment_groupby() {
    int num_rows = 1 << 20;
    int num_experiments = 11;

    log_set_quiet(true);

    auto results_path = "groupby.csv";
    FILE *results = fopen(results_path, "w");
    fprintf(results,
            "experiment,group_columns,zero_prefix,input_size,output_size,column_comparisons,rows_written,duration\n");

    for (int group_columns = 3; group_columns < ROW_ARITY - 1; group_columns++) {
        for (int zero_prefix = 0; zero_prefix < group_columns - 2; zero_prefix++) {
            for (int i = 0; i < num_experiments; i++) {
                log_set_quiet(false);
                log_info("group_by with num_rows=%lu, group_columns=%d, zero_prefix=%d output_size=%lu",
                         num_rows, group_columns, zero_prefix, num_rows >> i);
                log_set_quiet(true);


                auto agg = aggregates::Avg(group_columns, group_columns);

                auto gen = DuplicateGenerator(num_rows, 64, 1 << i, zero_prefix, group_columns, 1337);

                auto hash = HashGroupBy(gen.clone(), group_columns, agg);

                auto in_stream_scan = InStreamGroupByNoOvc(new Sort(gen.clone()), group_columns, agg);

                // we only sort by the group-by columns here
                auto in_stream_sort_ovc = InStreamGroupBy(
                        new SortPrefix(gen.clone(), group_columns),
                        group_columns,
                        agg);

                auto in_stream_sort_no_ovc = InStreamGroupByNoOvc(
                        new SortPrefixNoOVC(gen.clone(), group_columns),
                        group_columns,
                        agg);

                auto in_sort_ovc = InSortGroupBy(gen.clone(), group_columns, agg);

                auto in_sort_no_ovc = InSortGroupByNoOvc(gen.clone(), group_columns, agg);

                {
                    auto t0 = now();
                    hash.run();
                    auto hash_duration = since(t0);

                    fprintf(results, "%s,%d,%d,%d,%lu,%lu,%zu,%lu\n", "hash", group_columns, zero_prefix, num_rows,
                            hash.getCount(), hash.getStats().column_comparisons, hash.getStats().rows_written,
                            hash_duration);
                    fflush(results);
                }
/*
                {
                auto t0 = now();
                in_stream_scan.run();
                auto in_stream_scan_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_stream_scan", group_columns, zero_prefix,
                        num_rows,
                        in_stream_scan.getCount(),
                        in_stream_scan.getStats().column_comparisons, in_stream_scan.getStats().rows_written,
                        in_stream_scan_duration);
                        }
*/

                {

                    auto t0 = now();
                    in_sort_ovc.run();
                    auto in_sort_ovc_duration = since(t0);

                    fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_sort_ovc", group_columns, zero_prefix,
                            num_rows,
                            in_sort_ovc.getCount(),
                            in_sort_ovc.getStats().column_comparisons, in_sort_ovc.getStats().rows_written,
                            in_sort_ovc_duration);
                    fflush(results);
                }
/*
  {
                auto t0 = now();
                in_stream_sort_ovc.run();
                auto in_stream_sort_ovc_duration = since(t0);

                fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_stream_sort_ovc", group_columns, zero_prefix,
                        num_rows, in_stream_sort_ovc.getCount(),
                        in_stream_sort_ovc.getStats().column_comparisons +
                        in_stream_sort_ovc.getInput<SortPrefix>()->getStats().column_comparisons,
                        in_stream_sort_ovc.getInput<SortPrefix>()->getStats().rows_written,
                        in_stream_sort_ovc_duration);
                        }
*/
                {

                    auto t0 = now();
                    in_sort_no_ovc.run();
                    auto in_sort_no_ovc_duration = since(t0);

                    fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_sort_no_ovc", group_columns, zero_prefix,
                            num_rows,
                            in_sort_no_ovc.getCount(),
                            in_sort_no_ovc.getStats().column_comparisons, in_sort_no_ovc.getStats().rows_written,
                            in_sort_no_ovc_duration);
                    fflush(results);
                }

                {
                    auto t0 = now();
                    in_stream_sort_no_ovc.run();
                    auto in_stream_sort_no_ovc_duration = since(t0);

                    fprintf(results, "%s,%d,%d,%d,%lu,%lu,%lu,%lu\n", "in_stream_sort_no_ovc", group_columns,
                            zero_prefix,
                            num_rows, in_stream_sort_no_ovc.getCount(),
                            in_stream_sort_no_ovc.getStats().column_comparisons +
                            in_stream_sort_no_ovc.getInput()->getStats().column_comparisons,
                            in_stream_sort_no_ovc.getInput()->getStats().rows_written,
                            in_stream_sort_no_ovc_duration);
                    fflush(results);
                }
            }
        }
    }

    fclose(results);
    log_set_quiet(false);
}

void experiment_joins4() {
    // unique right input
    int left_exp = 20;
    int right_exp = 21;
    int left_input_size = 1 << left_exp;
    int right_input_size = 1 << right_exp;
    int num_experiments = 11;

    int domains[] = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            /*10*/, 0, 0, 0, 0, 0,
            /*16*/ 16,
            /*17*/ 20,
            /*18*/ 24,
            /*19*/ 28,
            /*20*/ 34,
            /*21*/ 40
    };

    const int join_columns = 4;
    int zero_prefix = 0;

    log_set_quiet(true);

    auto results_path = "joins4_" + std::to_string(left_exp) + "_" + std::to_string(right_exp) + ".csv";

    FILE *results = fopen(results_path.c_str(), "w");
    fprintf(results,
            "experiment,join_columns,zero_prefix,randomness,left_input_size,right_input_size,output_size,column_comparisons,rows_written,duration,apply_left,apply_right,join\n");

    for (int i = 0; i < num_experiments; i++) {
        log_set_quiet(false);
        log_info("join with left_input_size=%lu, right_input_size=%lu join_columns=%d",
                 left_input_size, right_input_size, join_columns);
        log_set_quiet(true);

        auto domain = i * 4 + domains[right_exp];
        auto gen1 = RowGeneratorWithDomains(left_input_size, domain, 0, 1337);
        auto gen2 = UniqueRowGenerator(right_input_size, domain, 0, join_columns, 1337 + 1);

        auto ovc_join_only = LeftSemiJoin(new SortPrefix(gen1.clone(), join_columns),
                                          new SortPrefix(gen2.clone(), join_columns), join_columns);

        auto ovc_two_inputs = LeftSemiJoin(
                new OVCApplier(new SortPrefix(gen1.clone(), join_columns), join_columns),
                new OVCApplier(new SortPrefix(gen2.clone(), join_columns), join_columns),
                join_columns);

        auto traditional = LeftSemiJoinNoOVC(new SortPrefix(gen1.clone(), join_columns),
                                             new SortPrefix(gen2.clone(), join_columns), join_columns);

        auto hash = LeftSemiHashJoin(gen1.clone(), gen2.clone(), join_columns);

        {
            auto t0 = now();
            ovc_join_only.run();
            auto duration = since(t0);

            struct iterator_stats stats = {0};
            ovc_join_only.accumulateStats(stats);

            fprintf(results, "%s,%d,%d,%d,%d,%d,%ld,%lu,%zu,%lu,%d,%d,%d\n", "ovc_join_only", join_columns,
                    zero_prefix,
                    domain,
                    left_input_size, right_input_size,
                    ovc_join_only.getCount(), ovc_join_only.getStats().column_comparisons,
                    stats.rows_written,
                    duration,
                    0, 0, 0
            );
            fflush(results);
        }

        {
            auto t0 = now();
            ovc_two_inputs.run();
            auto duration = since(t0);

            struct iterator_stats stats = {0};
            ovc_two_inputs.accumulateStats(stats);

            fprintf(results, "%s,%d,%d,%d,%d,%d,%ld,%lu,%zu,%lu,%zu,%zu,%zu\n", "ovc_two_inputs", join_columns,
                    zero_prefix, domain,
                    left_input_size, right_input_size,
                    ovc_two_inputs.getCount(),
                    ovc_two_inputs.getStats().column_comparisons +
                    ovc_two_inputs.getLeftInput<OVCApplier>()->getStats().column_comparisons +
                    ovc_two_inputs.getRightInput<OVCApplier>()->getStats().column_comparisons,
                    stats.rows_written,
                    duration,
                    ovc_two_inputs.getLeftInput<OVCApplier>()->getStats().column_comparisons,
                    ovc_two_inputs.getRightInput<OVCApplier>()->getStats().column_comparisons,
                    ovc_two_inputs.getStats().column_comparisons
            );
            fflush(results);
        }

        {
            auto t0 = now();
            traditional.run();
            auto duration = since(t0);

            struct iterator_stats stats = {0};
            traditional.accumulateStats(stats);

            fprintf(results, "%s,%d,%d,%d,%d,%d,%ld,%lu,%zu,%lu,%d,%d,%d\n", "traditional", join_columns, zero_prefix,
                    domain,
                    left_input_size, right_input_size,
                    traditional.getCount(),
                    traditional.getStats().column_comparisons,
                    stats.rows_written,
                    duration,
                    0, 0, 0
            );
            fflush(results);
        }
/*
        {
            auto t0 = now();
            hash.run();
            auto duration = since(t0);

            struct iterator_stats stats = {0};
            hash.accumulateStats(stats);

            fprintf(results, "%s,%d,%d,%d,%d,%d,%ld,%lu,%zu,%lu,%d\n", "hash", join_columns, zero_prefix,
                    randomness,
                    left_input_size, right_input_size,
                    hash.getCount(),
                    hash.getStats().column_comparisons,
                    stats.rows_written,
                    duration,
                    0
            );
            fflush(results);
        }
 */
    }

    fclose(results);
    log_set_quiet(false);
}

void experiment_joins5() {
    int domains[] = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            /*10*/, 0, 0, 0, 0,
            /*15*/ 10,
            /*16*/ 12,
            /*17*/ 16,
            /*18*/ 16,
            /*19*/ 20,
            /*20*/ 20,
            /*21*/ 24
    };

    const int join_columns = 4;
    int zero_prefix = 0;

    log_set_quiet(true);

    FILE *results = fopen("joins5.csv", "w");
    fprintf(results,
            "experiment,join_columns,zero_prefix,randomness,left_input_size,right_input_size,output_size,column_comparisons,rows_written,duration,apply_left,apply_right,join\n");
    for (int right_exp = 15; right_exp < 22; right_exp++) {

        int left_exp = 20;
        int left_input_size = 1 << left_exp;
        int right_input_size = 1 << right_exp;
        int num_experiments = 10;

        for (int i = 0; i < num_experiments; i++) {
            log_set_quiet(false);
            log_info("join with left_input_size=%lu, right_input_size=%lu join_columns=%d",
                     left_input_size, right_input_size, join_columns);
            log_set_quiet(true);

            auto domain = i * 4 + domains[right_exp];
            auto gen1 = RowGeneratorWithDomains(left_input_size, domain, 0, 1337);
            auto gen2 = RowGeneratorWithDomains(right_input_size, domain, 0, 1337 + 1);

            auto ovc_join_only = LeftSemiJoin(new SortPrefix(gen1.clone(), join_columns),
                                              new SortPrefix(gen2.clone(), join_columns), join_columns);

            auto ovc_two_inputs = LeftSemiJoin(
                    new OVCApplier(new SortPrefix(gen1.clone(), join_columns), join_columns),
                    new OVCApplier(new SortPrefix(gen2.clone(), join_columns), join_columns),
                    join_columns);

            auto traditional = LeftSemiJoinNoOVC(new SortPrefix(gen1.clone(), join_columns),
                                                 new SortPrefix(gen2.clone(), join_columns), join_columns);

            auto hash = LeftSemiHashJoin(gen1.clone(), gen2.clone(), join_columns);

            {
                auto t0 = now();
                ovc_join_only.run();
                auto duration = since(t0);

                struct iterator_stats stats = {0};
                ovc_join_only.accumulateStats(stats);

                fprintf(results, "%s,%d,%d,%d,%d,%d,%ld,%lu,%zu,%lu,%d,%d,%d\n", "ovc_join_only", join_columns,
                        zero_prefix,
                        domain,
                        left_input_size, right_input_size,
                        ovc_join_only.getCount(), ovc_join_only.getStats().column_comparisons,
                        stats.rows_written,
                        duration,
                        0, 0, 0
                );
                fflush(results);
            }

            {
                auto t0 = now();
                ovc_two_inputs.run();
                auto duration = since(t0);

                struct iterator_stats stats = {0};
                ovc_two_inputs.accumulateStats(stats);

                fprintf(results, "%s,%d,%d,%d,%d,%d,%ld,%lu,%zu,%lu,%zu,%zu,%zu\n", "ovc_two_inputs", join_columns,
                        zero_prefix, domain,
                        left_input_size, right_input_size,
                        ovc_two_inputs.getCount(),
                        ovc_two_inputs.getStats().column_comparisons +
                        ovc_two_inputs.getLeftInput<OVCApplier>()->getStats().column_comparisons +
                        ovc_two_inputs.getRightInput<OVCApplier>()->getStats().column_comparisons,
                        stats.rows_written,
                        duration,
                        ovc_two_inputs.getLeftInput<OVCApplier>()->getStats().column_comparisons,
                        ovc_two_inputs.getRightInput<OVCApplier>()->getStats().column_comparisons,
                        ovc_two_inputs.getStats().column_comparisons
                );
                fflush(results);
            }

            {
                auto t0 = now();
                traditional.run();
                auto duration = since(t0);

                struct iterator_stats stats = {0};
                traditional.accumulateStats(stats);

                fprintf(results, "%s,%d,%d,%d,%d,%d,%ld,%lu,%zu,%lu,%d,%d,%d\n", "traditional", join_columns,
                        zero_prefix,
                        domain,
                        left_input_size, right_input_size,
                        traditional.getCount(),
                        traditional.getStats().column_comparisons,
                        stats.rows_written,
                        duration,
                        0, 0, 0
                );
                fflush(results);
            }
/*
            {
                auto t0 = now();
                hash.run();
                auto duration = since(t0);

                struct iterator_stats stats = {0};
                hash.accumulateStats(stats);

                fprintf(results, "%s,%d,%d,%d,%d,%d,%ld,%lu,%zu,%lu,%d\n", "hash", join_columns, zero_prefix,
                        randomness,
                        left_input_size, right_input_size,
                        hash.getCount(),
                        hash.getStats().column_comparisons,
                        stats.rows_written,
                        duration,
                        0
                );
                fflush(results);
            }
        */
        }

    }
    fclose(results);
    log_set_quiet(false);
}

void experiment_column_order_sort() {
    int num_rows = 1 << 20;

    FILE *results = fopen("column_order_sort.csv", "w");
    fprintf(results, "num_rows,pos,experiment,column_comparisons\n");

    for (int pos = 0; pos < ROW_ARITY; pos++) {
        uint8_t bits[] = {1, 1, 1, 1, 1, 1, 1, 1};
        bits[pos] = 64 - 7 * 1;

        auto gen = RowGenerator(num_rows, bits);

        {
            auto plan1 = Sort(gen.clone());
            plan1.run();
            fprintf(results, "%d,%d,%s,%zu\n", num_rows, pos, "ovc", plan1.getStats().column_comparisons + num_rows);
        }
        {
            auto plan2 = SortNoOVC(gen.clone());
            plan2.run();
            fprintf(results, "%d,%d,%s,%zu\n", num_rows, pos, "no_ovc", plan2.getStats().column_comparisons);
        }
    }
    fclose(results);
}

void experiment_complex() {
    int num_rows = 1 << 20;
    int sort_columns = ROW_ARITY;

#define NBITS 6
#define STR_INDIR(X) #X
#define STR(X) STR_INDIR(X)

    FILE *results = fopen("complex"
                          "_" STR(NBITS)
                          ".csv", "w");
    fprintf(results, "num_rows,experiment,plan,presorted,column_comparisons,hashes,column_accesses\n");

    uint8_t bits[] = {7, 7, 7, 7, 7, 7, 7, 7};
    memset(bits, NBITS, sizeof bits);

    int seed = 1337;
    auto gen1 = RowGenerator(num_rows, bits, seed++);
    auto gen2 = RowGenerator(num_rows, bits, seed++);
    auto gen3 = RowGenerator(num_rows, bits, seed++);
    auto gen4 = RowGenerator(num_rows, bits, seed++);
    auto gen5 = RowGenerator(num_rows, bits, seed++);

    int first_join_columns = 3;
    int second_join_columns = 3;
    int third_join_columns = 3;

    for (int presorted = 0; presorted <= 5; presorted++) {
        {
            auto sort = LeftSemiJoin(
                    new SortPrefix(
                            new Transposer(
                                    new LeftSemiJoin(
                                            new LeftSemiJoin(
                                                    (new SortPrefix(gen1.clone(), first_join_columns))->disableStats(
                                                            presorted > 0),
                                                    (new SortPrefix(gen2.clone(), first_join_columns))->disableStats(
                                                            presorted > 1),
                                                    first_join_columns),
                                            (new SortPrefix(gen3.clone(), first_join_columns))->disableStats(
                                                    presorted > 2),
                                            first_join_columns
                                    ),
                                    first_join_columns),
                            third_join_columns),
                    new SortPrefix(
                            new Transposer(
                                    new LeftSemiJoin(
                                            (new SortPrefix(gen4.clone(), second_join_columns))->disableStats(
                                                    presorted > 3),
                                            (new SortPrefix(gen5.clone(), second_join_columns))->disableStats(
                                                    presorted > 4),
                                            second_join_columns),
                                    second_join_columns),
                            third_join_columns),
                    third_join_columns
            );
            sort.run();
            struct iterator_stats acc = {};
            sort.accumulateStats(acc);

            fprintf(results, "%d,%s,%s,%d,%zu,%zu,%zu\n", num_rows, "ovc", "plan1", presorted, acc.column_comparisons,
                    (size_t) 0,
                    2 * acc.column_comparisons);
            fflush(results);
        }
        {
            auto sort = LeftSemiJoin(
                    new LeftSemiJoin(
                            new LeftSemiJoin(
                                    (new SortPrefix(gen1.clone(), first_join_columns))->disableStats(
                                            presorted > 0),
                                    (new SortPrefix(gen2.clone(), first_join_columns))->disableStats(
                                            presorted > 1),
                                    first_join_columns),
                            (new SortPrefix(gen3.clone(), first_join_columns))->disableStats(
                                    presorted > 2),
                            first_join_columns
                    ),
                    new SortPrefix(
                            new Transposer(
                                    new LeftSemiJoin((new SortPrefix(gen4.clone(), second_join_columns))->disableStats(
                                                             presorted > 3),
                                                     (new SortPrefix(gen5.clone(), second_join_columns))->disableStats(
                                                             presorted > 4),
                                                     second_join_columns),
                                    second_join_columns),
                            third_join_columns),
                    third_join_columns
            );
            sort.run();
            struct iterator_stats acc = {};
            sort.accumulateStats(acc);

            fprintf(results, "%d,%s,%s,%d,%zu,%zu,%zu\n", num_rows, "ovc", "plan2", presorted, acc.column_comparisons,
                    (size_t) 0,
                    2 * acc.column_comparisons);
            fflush(results);
        }
        {
            auto sort = LeftSemiJoin(
                    new LeftSemiJoin(
                            new LeftSemiJoin(
                                    (new SortPrefix(gen1.clone(), first_join_columns))->disableStats(presorted > 0),
                                    (new SortPrefix(gen2.clone(), first_join_columns))->disableStats(presorted > 1),
                                    first_join_columns),
                            (new SortPrefix(gen3.clone(), first_join_columns))->disableStats(presorted > 2),
                            first_join_columns
                    ),
                    new LeftSemiJoin(
                            (new SortPrefix(gen4.clone(), second_join_columns))->disableStats(presorted > 3),
                            (new SortPrefix(gen5.clone(), second_join_columns))->disableStats(presorted > 4),
                            second_join_columns),
                    second_join_columns
            );
            sort.run();
            struct iterator_stats acc = {};
            sort.accumulateStats(acc);

            fprintf(results, "%d,%s,%s,%d,%zu,%zu,%zu\n", num_rows, "ovc", "plan3", presorted, acc.column_comparisons,
                    (size_t) 0,
                    2 * acc.column_comparisons);
            fflush(results);
        }

        {
            auto sort = LeftSemiJoinNoOVC(
                    new SortPrefixNoOVC(
                            new Transposer(
                                    new LeftSemiJoinNoOVC(
                                            new LeftSemiJoinNoOVC(
                                                    (new SortPrefixNoOVC(gen1.clone(),
                                                                         first_join_columns))->disableStats(
                                                            presorted > 0),
                                                    (new SortPrefixNoOVC(gen2.clone(),
                                                                         first_join_columns))->disableStats(
                                                            presorted > 1),
                                                    first_join_columns),
                                            (new SortPrefixNoOVC(gen3.clone(), first_join_columns))->disableStats(
                                                    presorted > 2),
                                            first_join_columns
                                    ),
                                    first_join_columns),
                            third_join_columns),
                    new SortPrefixNoOVC(
                            new Transposer(
                                    new LeftSemiJoinNoOVC(
                                            (new SortPrefixNoOVC(gen4.clone(), second_join_columns))->disableStats(
                                                    presorted > 3),
                                            (new SortPrefixNoOVC(gen5.clone(), second_join_columns))->disableStats(
                                                    presorted > 4),
                                            second_join_columns),
                                    second_join_columns),
                            third_join_columns),
                    third_join_columns
            );
            sort.run();
            struct iterator_stats acc = {};
            sort.accumulateStats(acc);

            fprintf(results, "%d,%s,%s,%d,%zu,%zu,%zu\n", num_rows, "no_ovc", "plan1", presorted,
                    acc.column_comparisons,
                    (size_t) 0,
                    2 * acc.column_comparisons);
            fflush(results);
        }

        {
            auto sort = LeftSemiJoinNoOVC(
                    new LeftSemiJoinNoOVC(
                            new LeftSemiJoinNoOVC(
                                    (new SortPrefixNoOVC(gen1.clone(), first_join_columns))->disableStats(
                                            presorted > 0),
                                    (new SortPrefixNoOVC(gen2.clone(), first_join_columns))->disableStats(
                                            presorted > 1),
                                    first_join_columns),
                            (new SortPrefixNoOVC(gen3.clone(), first_join_columns))->disableStats(
                                    presorted > 2),
                            first_join_columns
                    ),
                    new SortPrefixNoOVC(
                            new Transposer(
                                    new LeftSemiJoinNoOVC(
                                            (new SortPrefixNoOVC(gen4.clone(), second_join_columns))->disableStats(
                                                    presorted > 3),
                                            (new SortPrefixNoOVC(gen5.clone(), second_join_columns))->disableStats(
                                                    presorted > 4),
                                            second_join_columns),
                                    second_join_columns),
                            third_join_columns),
                    third_join_columns
            );
            sort.run();
            struct iterator_stats acc = {};
            sort.accumulateStats(acc);

            fprintf(results, "%d,%s,%s,%d,%zu,%zu,%zu\n", num_rows, "no_ovc", "plan2", presorted,
                    acc.column_comparisons,
                    (size_t) 0,
                    2 * acc.column_comparisons);
            fflush(results);
        }
        {
            auto sort = LeftSemiJoinNoOVC(
                    new LeftSemiJoinNoOVC(
                            new LeftSemiJoinNoOVC(
                                    (new SortPrefixNoOVC(gen1.clone(), first_join_columns))->disableStats(
                                            presorted > 0),
                                    (new SortPrefixNoOVC(gen2.clone(), first_join_columns))->disableStats(
                                            presorted > 1),
                                    first_join_columns),
                            (new SortPrefixNoOVC(gen3.clone(), first_join_columns))->disableStats(presorted > 2),
                            first_join_columns
                    ),
                    new LeftSemiJoinNoOVC(
                            (new SortPrefixNoOVC(gen4.clone(), second_join_columns))->disableStats(presorted > 3),
                            (new SortPrefixNoOVC(gen5.clone(), second_join_columns))->disableStats(presorted > 4),
                            second_join_columns),
                    second_join_columns
            );
            sort.run();
            struct iterator_stats acc = {};
            sort.accumulateStats(acc);

            fprintf(results, "%d,%s,%s,%d,%zu,%zu,%zu\n", num_rows, "no_ovc", "plan3", presorted,
                    acc.column_comparisons,
                    (size_t) 0,
                    2 * acc.column_comparisons);
            fflush(results);
        }

        {
            auto hash = LeftSemiHashJoin(
                    new Transposer(
                            new LeftSemiHashJoin(
                                    new LeftSemiHashJoin(
                                            gen1.clone(),
                                            gen2.clone(),
                                            first_join_columns),
                                    gen3.clone(),
                                    first_join_columns
                            ),
                            first_join_columns),
                    new Transposer(
                            new LeftSemiHashJoin(
                                    gen4.clone(),
                                    gen5.clone(),
                                    second_join_columns),
                            second_join_columns),
                    third_join_columns
            );
            hash.run();

            struct iterator_stats acc = {};
            hash.accumulateStats(acc);

            fprintf(results, "%d,%s,%s,%d,%zu,%zu,%zu\n", num_rows, "hash", "plan1", presorted, acc.column_comparisons,
                    acc.columns_hashed,
                    2 * acc.column_comparisons + acc.columns_hashed);
            fflush(results);
        }

        {
            auto hash = LeftSemiHashJoin(
                    new LeftSemiHashJoin(
                            new LeftSemiHashJoin(
                                    gen1.clone(),
                                    gen2.clone(),
                                    first_join_columns),
                            gen3.clone(),
                            first_join_columns
                    ),
                    new Transposer(
                            new LeftSemiHashJoin(
                                    gen4.clone(),
                                    gen5.clone(),
                                    second_join_columns),
                            second_join_columns),
                    third_join_columns
            );
            hash.run();

            struct iterator_stats acc = {};
            hash.accumulateStats(acc);

            fprintf(results, "%d,%s,%s,%d,%zu,%zu,%zu\n", num_rows, "hash", "plan2", presorted, acc.column_comparisons,
                    acc.columns_hashed,
                    2 * acc.column_comparisons + acc.columns_hashed);
            fflush(results);
        }
        {
            auto hash = LeftSemiHashJoin(
                    new LeftSemiHashJoin(
                            new LeftSemiHashJoin(
                                    gen1.clone(),
                                    gen2.clone(),
                                    first_join_columns),
                            gen3.clone(),
                            first_join_columns
                    ),
                    new LeftSemiHashJoin(
                            gen4.clone(),
                            gen5.clone(),
                            second_join_columns),
                    second_join_columns
            );
            hash.run();

            struct iterator_stats acc = {};
            hash.accumulateStats(acc);

            fprintf(results, "%d,%s,%s,%d,%zu,%zu,%zu\n", num_rows, "hash", "plan3", presorted, acc.column_comparisons,
                    acc.columns_hashed,
                    2 * acc.column_comparisons + acc.columns_hashed);
            fflush(results);
        }
    }

    fclose(results);
}

void experiment_complex2() {
    int num_rows = 1 << 20;
    int domain = 80;
    int group_columns = 3;

    auto gen1 = RowGeneratorWithDomains(num_rows, domain, 0);
    auto gen2 = RowGeneratorWithDomains(num_rows, domain, 0, 1337 + 1);

    auto size = LeftSemiJoin(
            new SortPrefix(gen1.clone(), group_columns),
            new SortPrefix(gen2.clone(), group_columns),
            group_columns).collect().size();

    auto plan_ovc = InStreamGroupBy(
            new LeftSemiJoin(
                    new SortPrefix(gen1.clone(), group_columns),
                    new SortPrefix(gen2.clone(), group_columns),
                    group_columns),
            group_columns,
            aggregates::Count(group_columns));

    auto plan_sort = InStreamGroupByNoOvc(
            new LeftSemiJoin(
                    new SortPrefix(gen1.clone(), group_columns),
                    new SortPrefix(gen2.clone(), group_columns),
                    group_columns),
            group_columns,
            aggregates::Count(group_columns));

    auto plan_hash = HashGroupBy(
            new LeftSemiHashJoin(
                    gen1.clone(),
                    gen2.clone(),
                    group_columns),
            group_columns,
            aggregates::Count(group_columns));

    plan_ovc.run();
    plan_sort.run();
    plan_hash.run();

    iterator_stats stats_ovc = {0};
    iterator_stats stats_sort = {0};
    iterator_stats stats_hash = {0};
    plan_ovc.accumulateStats(stats_ovc);
    plan_sort.accumulateStats(stats_sort);
    plan_hash.accumulateStats(stats_hash);

    FILE *results = fopen("complex2.csv", "w");
    fprintf(results, "experiment,join_output,column_comparisons\n");

    fprintf(results, "%s,%lu,%lu\n", "ovc", size, stats_ovc.column_comparisons);
    fprintf(results, "%s,%lu,%lu\n", "traditional", size, stats_sort.column_comparisons);
    fprintf(results, "%s,%lu,%lu\n", "hash", size, stats_hash.column_comparisons);

    fclose(results);
}

void experiment_complex3() {
    int num_rows = 1 << 20;
    int domain = 64;
    int group_columns = 3;

    auto gen1 = RowGeneratorWithDomains(num_rows, domain, 0);
    auto gen2 = RowGeneratorWithDomains(num_rows, domain, 0, 1337 + 1);

    auto plan_ovc =
            LeftSemiJoin(
                    new SortDistinctPrefix(gen1.clone(), group_columns),
                    new SortDistinctPrefix(gen2.clone(), group_columns),
                    group_columns);

    auto plan_sort =
            LeftSemiJoinNoOVC(
                    new SortDistinctPrefix(gen1.clone(), group_columns),
                    new SortDistinctPrefix(gen2.clone(), group_columns),
                    group_columns);

    auto plan_hash =
            LeftSemiHashJoin(
                    new HashDistinct(gen1.clone(), group_columns),
                    new HashDistinct(gen2.clone(), group_columns),
                    group_columns);

    plan_ovc.run();
    plan_sort.run();
    plan_hash.run();

    iterator_stats stats_ovc = {0};
    iterator_stats stats_sort = {0};
    iterator_stats stats_hash = {0};
    plan_ovc.accumulateStats(stats_ovc);
    plan_sort.accumulateStats(stats_sort);
    plan_hash.accumulateStats(stats_hash);

    auto size = plan_ovc.getCount();

    FILE *results = fopen("complex3.csv", "w");
    fprintf(results, "experiment,join_output,column_comparisons\n");

    fprintf(results, "%s,%lu,%lu\n", "ovc", size, stats_ovc.column_comparisons);
    fprintf(results, "%s,%lu,%lu\n", "traditional", size, stats_sort.column_comparisons);
    fprintf(results, "%s,%lu,%lu\n", "hash", size, stats_hash.column_comparisons);

    fclose(results);
}

void experiment_complex4() {
    int num_rows = 1 << 20;
    int domain = 80;
    int group_columns = 3;

    auto gen1 = RowGeneratorWithDomains(num_rows, domain, 0);
    auto gen2 = RowGeneratorWithDomains(num_rows, domain, 0, 1337 + 1);

    auto size = LeftSemiJoin(
            new SortPrefix(gen1.clone(), group_columns),
            new SortPrefix(gen2.clone(), group_columns),
            group_columns).collect().size();

    auto plan_ovc = InStreamGroupBy(
            new LeftSemiJoin(
                    new SortPrefix(gen1.clone(), group_columns),
                    new SortPrefix(gen2.clone(), group_columns),
                    group_columns),
            group_columns,
            aggregates::Count(group_columns));

    auto plan_sort = InStreamGroupByNoOvc(
            new LeftSemiJoinNoOVC(
                    new SortPrefix(gen1.clone(), group_columns),
                    new SortPrefix(gen2.clone(), group_columns),
                    group_columns),
            group_columns,
            aggregates::Count(group_columns));

    auto plan_hash = HashGroupBy(
            new LeftSemiHashJoin(
                    gen1.clone(),
                    gen2.clone(),
                    group_columns),
            group_columns,
            aggregates::Count(group_columns));

    plan_ovc.run();
    plan_sort.run();
    plan_hash.run();

    iterator_stats stats_ovc = {0};
    iterator_stats stats_sort = {0};
    iterator_stats stats_hash = {0};
    plan_ovc.accumulateStats(stats_ovc);
    plan_sort.accumulateStats(stats_sort);
    plan_hash.accumulateStats(stats_hash);

    FILE *results = fopen("complex4.csv", "w");
    fprintf(results, "experiment,join_output,column_comparisons\n");

    fprintf(results, "%s,%lu,%lu\n", "ovc", size, stats_ovc.column_comparisons);
    fprintf(results, "%s,%lu,%lu\n", "traditional", size, stats_sort.column_comparisons);
    fprintf(results, "%s,%lu,%lu\n", "hash", size, stats_hash.column_comparisons);

    fclose(results);
}

void experiment_complex4_param() {
    int num_rows = 1 << 20;
    int group_columns = 3;

    int domains[] = {104, 128, 192, 256, 512};
    int num_experiments = sizeof domains / sizeof *domains;

    FILE *results = fopen("complex4p.csv", "w");
    fprintf(results, "experiment,output_size,join_output,column_comparisons\n");

    for (int i = 0; i < num_experiments; i++) {
        int domain = domains[i];

        //auto gen1 = RowGeneratorWithDomains(num_rows, domain, 0);
        //auto gen2 = RowGeneratorWithDomains(num_rows, domain, 0, 1337 + 1);

        auto gen1 = RowGeneratorWithDomains(num_rows << 1, domain, 0);
        auto gen2 = UniqueRowGenerator(num_rows, domain, 0, group_columns, 1337 + 1);

        auto join_size = LeftSemiJoin(
                new SortPrefix(gen1.clone(), group_columns),
                new SortPrefix(gen2.clone(), group_columns),
                group_columns).collect().size();

        auto plan_ovc = InStreamGroupBy(
                new LeftSemiJoin(
                        new SortPrefix(gen1.clone(), group_columns),
                        new SortPrefix(gen2.clone(), group_columns),
                        group_columns),
                group_columns,
                aggregates::Count(group_columns));

        auto plan_sort = InStreamGroupByNoOvc(
                new LeftSemiJoinNoOVC(
                        new SortPrefix(gen1.clone(), group_columns),
                        new SortPrefix(gen2.clone(), group_columns),
                        group_columns),
                group_columns,
                aggregates::Count(group_columns));

        auto plan_hash = HashGroupBy(
                new LeftSemiHashJoin(
                        gen1.clone(),
                        gen2.clone(),
                        group_columns),
                group_columns,
                aggregates::Count(group_columns));

        plan_ovc.run();
        plan_sort.run();
        plan_hash.run();

        iterator_stats stats_ovc = {0};
        iterator_stats stats_sort = {0};
        iterator_stats stats_hash = {0};
        plan_ovc.accumulateStats(stats_ovc);
        plan_sort.accumulateStats(stats_sort);
        plan_hash.accumulateStats(stats_hash);

        auto output_size = plan_sort.getCount();

        fprintf(results, "%s,%lu,%lu,%lu\n", "traditional", output_size, join_size, stats_sort.column_comparisons);
        fprintf(results, "%s,%lu,%lu,%lu\n", "ovc", output_size, join_size, stats_ovc.column_comparisons);
        fprintf(results, "%s,%lu,%lu,%lu\n", "hash", output_size, join_size, stats_hash.column_comparisons);
        fflush(results);
    }

    fclose(results);
}

int main(int argc, char *argv[]) {
    log_open(LOG_TRACE);
    log_set_quiet(false);
    log_set_level(LOG_TRACE);
    log_set_level(LOG_INFO);
    log_info("start", "");

    auto start = now();

    //experiment_groupby(); // here
    //experiment_joins4(); // here
    //experiment_joins5(); // here
    //experiment_column_order_sort();
    //experiment_complex();
    //experiment_complex2();
    //experiment_complex3();
    //experiment_complex4_param();

    //auto gen = RowGenerator(4, 2, 0, 1337);

    auto sort = SegmentedSortNoOVC(
            new SortPrefixNoOVC(new RowGeneratorWithDomains(1000, {10, 10, 1, 1, 1, 4, 4}, 1337), 2),
            CmpColumnListNoOVC({6, 5}),
            EqColumnListNoOVC({0, 1})
    );

    sort.run(true);

    log_info("elapsed=%lums", since(start));
    log_info("fin");
    log_close();
    return 0;
}