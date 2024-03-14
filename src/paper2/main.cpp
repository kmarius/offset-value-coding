#include "lib/comparators.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/GeneratorWithDomains.h"
#include "lib/iterators/SegmentedGen.h"
#include "lib/iterators/SegmentedSort.h"
#include "lib/iterators/SegmentedSortNoRuns.h"
#include "lib/iterators/Sort.h"
#include "lib/iterators/VectorGen.h"
#include "lib/log.h"
#include "lib/utils.h"

#include <thread>
#include <vector>

using namespace ovc;
using namespace ovc::iterators;

void experiment_sort_order1() {
    printf("experiment_sort_order1\n");

    const char *result_path = "sort_order1.csv";

    int num_rows = 1 << 20;
    int reps = 10;
    uint64_t bits_per_row = 20;
    uint64_t bitsA = 8;

#ifndef NDEBUG
    reps = 1;
#endif
#ifdef COLLECT_STATS
    result_path = "sort_order1_stats.csv";
    reps = 1;
#endif

    FILE *results = fopen(result_path, "w");
    fprintf(results, "experiment,data,num_rows,list_length,segments_found,runs_"
                     "generated,column_comparisons,duration\n");

    uint8_t A[ROW_ARITY] = {0};
    uint8_t B[ROW_ARITY] = {0};
    uint8_t AB[ROW_ARITY] = {0};
    uint8_t BA[ROW_ARITY] = {0};
    uint8_t *nil = AB;

    for (int list_length = 1; list_length <= 16; list_length *= 2) {
        printf("list_length=%d\n", list_length);
        const int key_length = list_length * 2;

        // initialize key column lists
        for (int i = 0; i < list_length; i++) {
            A[i] = BA[i + list_length] = AB[i] = i;
            B[i] = BA[i] = AB[i + list_length] = i + list_length;
        }

        uint64_t domains[3][ROW_ARITY];
        for (int i = 0; i < ROW_ARITY; i++) {
            for (auto &domain: domains) {
                domain[i] = 1;
            }
        }

        uint64_t bitsB = bits_per_row - bitsA;
        assert(bits_per_row >= bitsA);

        domains[1][list_length - 1] = 1 << bitsA; // A
        domains[1][key_length - 1] = 1 << bitsB;  // B
        domains[2][0] = 1 << bitsA;               // A
        domains[2][list_length] = 1 << bitsB;     // B

        // generate data and sort by AB
        struct {
            Generator *gen;
            const char *name;
        } gens[] = {{new VectorGen(new SortPrefixOVC(
                new GeneratorWithDomains(num_rows, domains[1], 1337 + 1),
                key_length)),
                            "last_decides"},
                    {new VectorGen(new SortPrefixOVC(
                            new GeneratorWithDomains(num_rows, domains[2], 1337 + 2),
                            key_length)),
                            "first_decides"}};

        for (auto &gen: gens) {
            for (int i = 0; i < reps; i++) {
                auto ovc = new SegmentedSortOVC<2048>(gen.gen->clone(), AB, 0,
                                                      list_length, list_length);

                Iterator *plan = ovc;
#ifndef NDEBUG
                auto asserter = new AssertSorted(ovc, CmpColumnListOVC(BA, key_length));
                plan = asserter;
#endif

                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                auto t0 = now();
                plan->run();
                auto duration = since(t0);

#ifndef NDEBUG
                assert(asserter->getCount() == num_rows);
                assert(asserter->isSorted());
#endif

                fprintf(results, "%s,%s,%d,%d,%lu,%lu,%lu,%lu\n", "ovc", gen.name,
                        num_rows, list_length, ovc->getStats().segments_found,
                        ovc->getStats().runs_generated,
                        ovc->getStats().column_comparisons, duration);
                fflush(results);

                delete plan;
            }

            for (int i = 0; i < reps; i++) {
                auto traditional = new SegmentedSort<2048>(
                        gen.gen->clone(), nil, 0, A, list_length, BA, list_length);

                Iterator *plan = traditional;
#ifndef NDEBUG
                auto asserter =
                    new AssertSorted(traditional, CmpColumnList(BA, key_length));
                plan = asserter;
#endif

                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                auto t0 = now();
                plan->run();
                auto duration = since(t0);

#ifndef NDEBUG
                assert(asserter->getCount() == num_rows);
                assert(asserter->isSorted());
#endif

                fprintf(results, "%s,%s,%d,%d,%lu,%lu,%lu,%lu\n", "traditional",
                        gen.name, num_rows, list_length,
                        traditional->getStats().segments_found,
                        traditional->getStats().runs_generated,
                        traditional->getStats().column_comparisons, duration);
                fflush(results);

                delete plan;
            }
        }

        for (auto &gen: gens) {
            delete gen.gen;
        }
    }

    fclose(results);
}

void experiment_sort_order2() {
    printf("experiment_sort_order2\n");

    const char *result_path = "sort_order2.csv";

    int num_rows_exp = 20;
    int num_rows = 1 << num_rows_exp;
    int reps = 10;
    int list_length = 8;

#ifndef NDEBUG
    reps = 1;
#endif
#ifdef COLLECT_STATS
    result_path = "sort_order2_stats.csv";
  reps = 1;
#endif

    FILE *results = fopen(result_path, "w");
    fprintf(results, "experiment,num_rows,bits_per_row,segments_found,runs_"
                     "generated,column_comparisons,duration\n");

    uint8_t A[ROW_ARITY] = {0};
    uint8_t B[ROW_ARITY] = {0};
    uint8_t CB[ROW_ARITY] = {0};
    uint8_t ABC[ROW_ARITY] = {0};
    uint8_t ACB[ROW_ARITY] = {0};

    for (uint64_t bitsA = 1; bitsA < 20; bitsA += 2) {
        const int key_length = list_length * 3;

        // initialize key column lists
        for (int i = 0; i < list_length; i++) {
            A[i] = ABC[i] = ACB[i] = i;
            B[i] = CB[i + list_length] = ABC[i + list_length] =
            ACB[i + 2 * list_length] = i + list_length;
            CB[i] = ABC[i + 2 * list_length] = ACB[i + list_length] =
                    i + 2 * list_length;
        }

        // initialize domains, values are chosen so that as the segments shrink in size by 4,
        // the number of runs in each segment and the length of each run shrinks by 2
        uint64_t domain[3];
        uint8_t columns[3];

        size_t bits_total = num_rows_exp + 2;
        size_t maxB = 10;

        ssize_t bitsB = num_rows_exp - (bitsA / 2) - maxB - 1;
        if (bitsB < 0) {
            bitsB = 0;
        }
        if (bitsB > maxB) {
            bitsB = maxB;
        }

        ssize_t bitsC = bits_total - bitsA - bitsB;
        assert(bitsC >= 0);

        fprintf(stdout, "bitsA=%lu, bitsB=%lu, bitsC=%lu\n", bitsA, bitsB, bitsC);

        columns[0] = list_length - 1;
        columns[1] = 2 * list_length - 1;
        columns[2] = 3 * list_length - 1;

        domain[0] = 1 << bitsA;
        domain[1] = 1 << bitsB;
        domain[2] = 1 << bitsC;

        // generate data and sort by ABC
        auto gen = VectorGen(new SortPrefixOVC(
                new SegmentedGen(num_rows, domain, columns, 1337), key_length));

        for (int i = 0; i < reps; i++) {
            auto ovc = new SegmentedSortOVC<2048>(gen.clone(), ABC, list_length,
                                                  list_length, list_length);

            Iterator *plan = ovc;
#ifndef NDEBUG
            auto asserter = new AssertSorted(plan, CmpColumnListOVC(ACB, key_length));
            plan = asserter;
#endif

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto t0 = now();
            plan->run();
            auto duration = since(t0);

#ifndef NDEBUG
            assert(asserter->getCount() == num_rows);
            assert(asserter->isSorted());
#endif

            fprintf(results, "%s,%d,%lu,%lu,%lu,%lu,%lu\n", "ovc", num_rows, bitsA,
                    ovc->getStats().segments_found, ovc->getStats().runs_generated,
                    ovc->getStats().column_comparisons, duration);
            fflush(results);

            delete plan;
        }

        // un-segmented
        for (int i = 0; i < reps; i++) {
            auto no_segment = new UnSegmentedSortOVC<1 << 20>(
                    gen.clone(), ABC, list_length, list_length, list_length);

            Iterator *plan = no_segment;
#ifndef NDEBUG
            auto asserter = new AssertSorted(plan, CmpColumnListOVC(ACB, key_length));
            plan = asserter;
#endif

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto t0 = now();
            plan->run();
            auto duration = since(t0);

#ifndef NDEBUG
            assert(asserter->getCount() == num_rows);
            assert(asserter->isSorted());
#endif

            fprintf(results, "%s,%d,%lu,%lu,%lu,%lu,%lu\n", "no_segment", num_rows,
                    bitsA, no_segment->getStats().segments_found,
                    no_segment->getStats().runs_generated,
                    no_segment->getStats().column_comparisons, duration);
            fflush(results);

            delete plan;
        }

        // no-runs
        for (int i = 0; i < reps; i++) {
            auto no_runs = new SegmentedSortNoRunsOVC<1 << 20>(
                    gen.clone(), ABC, list_length, list_length, list_length);
            Iterator *plan = no_runs;

#ifndef NDEBUG
            auto asserter = new AssertSorted(no_runs, CmpColumnList(ACB, key_length));
            plan = asserter;
#endif

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto t0 = now();
            plan->run();
            auto duration = since(t0);

#ifndef NDEBUG
            assert(asserter->getCount() == num_rows);
            assert(asserter->isSorted());
#endif

            fprintf(results, "%s,%d,%lu,%lu,%lu,%lu,%lu\n", "no_runs", num_rows,
                    bitsA, no_runs->getStats().segments_found,
                    no_runs->getStats().runs_generated,
                    no_runs->getStats().column_comparisons, duration);
            fflush(results);

            delete plan;
        }
    }

    fclose(results);
}

int main(int argc, char *argv[]) {
    log_open(LOG_TRACE);
    log_set_quiet(false);
    log_set_level(LOG_TRACE);
    log_info("start");

    auto start = now();

    experiment_sort_order1();
    experiment_sort_order2();

    log_info("elapsed=%lums", since(start));
    log_info("fin");
    log_close();
    return 0;
}