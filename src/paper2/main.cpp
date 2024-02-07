#include "lib/utils.h"
#include "lib/iterators/Sort.h"
#include "lib/PriorityQueue.h"
#include "lib/log.h"
#include "lib/iterators/AssertSorted.h"
#include "lib/iterators/GeneratorWithDomains.h"
#include "lib/iterators/SegmentedSort.h"
#include "lib/comparators.h"
#include "lib/iterators/VectorGen.h"

#include <vector>
#include <thread>

using namespace ovc;
using namespace ovc::iterators;

void experiment_sort_order1() {
    int num_rows = 1 << 20;
    int reps = 10;

#ifndef NDEBUG
    reps = 1;
#endif

    FILE *results = fopen("sort_order1.csv", "w");
    fprintf(results, "%s,%s,%s,%s,%s\n", "experiment", "data", "list_length", "column_comparisons", "duration");

    uint8_t A[ROW_ARITY] = {0};
    uint8_t B[ROW_ARITY] = {0};
    uint8_t AB[ROW_ARITY] = {0};
    uint8_t BA[ROW_ARITY] = {0};
    uint8_t *nil = AB;

    for (int list_length = 1; list_length <= 16; list_length *= 2) {
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

        uint64_t bits_per_row = 16;
        uint64_t bits_per_column = bits_per_row / list_length;

        uint64_t domain = 1 << bits_per_column;
        auto combined_domain = (uint64_t) pow((double) domain, list_length);

        for (int i = 0; i < key_length; i++) {
            domains[0][i] = domain;
        }

        domains[1][list_length - 1] = QUEUE_CAPACITY; // A
        domains[1][key_length - 1] = combined_domain / QUEUE_CAPACITY; // B
        domains[2][0] = QUEUE_CAPACITY; // A
        domains[2][list_length] = combined_domain / QUEUE_CAPACITY; // B

        // generate data and sort by AB
        struct {
            Generator *gen;
            const char *name;
        } gens[] = {
//                {new VectorGen(
//                        new SortPrefixOVC(
//                                new GeneratorWithDomains(num_rows, domains[0], 1337),
//                                key_length)),
//                        "random"},
                {new VectorGen(
                        new SortPrefixOVC(
                                new GeneratorWithDomains(num_rows, domains[1], 1337 + 1),
                                key_length)),
                        "last_decides"},
                {new VectorGen(
                        new SortPrefixOVC(
                                new GeneratorWithDomains(num_rows, domains[2], 1337 + 2),
                                key_length)),
                        "first_decides"}
        };

        for (auto &gen: gens) {
            for (int i = 0; i < reps; i++) {
                auto ovc = new SegmentedSortOVC(
                        gen.gen->clone(),
                        AB, 0, list_length, list_length
                );

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

                fprintf(results, "%s,%s,%d,%lu,%lu\n", "ovc", gen.name, list_length, ovc->getStats().column_comparisons,
                        duration);
                fprintf(stdout, "%s,%s,%d,%lu,%lu\n", "ovc", gen.name, list_length, ovc->getStats().column_comparisons,
                        duration);
                fflush(results);

                delete plan;
            }

            for (int i = 0; i < reps; i++) {
                auto traditional = new
                        SegmentedSort(
                        gen.gen->clone(),
                        nil, 0,
                        A, list_length,
                        BA, 2 * list_length);

                Iterator *plan = traditional;
#ifndef NDEBUG
                auto asserter = new AssertSorted(traditional, CmpColumnList(BA, key_length));
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

                fprintf(results, "%s,%s,%d,%lu,%lu\n", "traditional", gen.name, list_length,
                        traditional->getStats().column_comparisons, duration);
                fprintf(stdout, "%s,%s,%d,%lu,%lu\n", "traditional", gen.name, list_length,
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
    int num_rows = 1 << 20;
    int reps = 10;
    int list_length = 8;

#ifndef NDEBUG
    reps = 1;
#endif

    FILE *results = fopen("sort_order2.csv", "w");
    fprintf(results, "%s,%s,%s,%s\n", "experiment", "bits_per_row", "column_comparisons", "duration");

    uint8_t A[ROW_ARITY] = {0};
    uint8_t B[ROW_ARITY] = {0};
    uint8_t CB[ROW_ARITY] = {0};
    uint8_t ABC[ROW_ARITY] = {0};
    uint8_t ACB[ROW_ARITY] = {0};

    for (uint64_t bitsA = 1; bitsA < 20; bitsA += 3) {
        const int key_length = list_length * 3;

        // initialize key column lists
        for (int i = 0; i < list_length; i++) {
            A[i] = ABC[i] = ACB[i] = i;
            B[i] = CB[i + list_length] = ABC[i + list_length] = ACB[i + 2 * list_length] = i + list_length;
            CB[i] = ABC[i + 2 * list_length] = ACB[i + list_length] = i + 2 * list_length;
        }

        uint64_t domains[1][ROW_ARITY];
        for (int i = 0; i < ROW_ARITY; i++) {
            for (auto &domain: domains) {
                domain[i] = 1;
            }
        }

        domains[0][list_length - 1] = 1 << bitsA;
        domains[0][2 * list_length - 1] = QUEUE_CAPACITY;
        domains[0][3 * list_length - 1] = 1024;

        // generate data and sort by ABC
        struct {
            Generator *gen;
            const char *name;
        } gens[] = {
                {new VectorGen(
                        new SortPrefixOVC(
                                new GeneratorWithDomains(num_rows, domains[0], 1337),
                                key_length)),
                 "last_decides"},
        };

        for (auto &gen: gens) {
            for (int i = 0; i < reps; i++) {
                auto ovc = new SegmentedSortOVC(
                        gen.gen->clone(),
                        ABC, list_length, list_length, list_length
                );

                Iterator *plan = ovc;
#ifndef NDEBUG
                auto asserter = new AssertSorted(plan, CmpColumnListOVC(ACB, key_length));
                plan = asserter;
#endif

                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                auto t0 = now();
                plan->run(true);
                auto duration = since(t0);

#ifndef NDEBUG
                assert(asserter->getCount() == num_rows);
                assert(asserter->isSorted());
#endif

                fprintf(results, "%s,%lu,%lu,%lu\n", "ovc", bitsA,
                        ovc->getStats().column_comparisons, duration);
                fprintf(stdout, "%s,%lu,%lu,%lu\n", "ovc", bitsA,
                        ovc->getStats().column_comparisons, duration);
                fflush(results);

                delete plan;
            }

            for (int i = 0; i < reps; i++) {
                auto traditional = new
                        SegmentedSort(
                        gen.gen->clone(),
                        A, list_length,
                        B, list_length,
                        CB, list_length);
                Iterator *plan = traditional;

#ifndef NDEBUG
                auto asserter = new AssertSorted(traditional, CmpColumnList(ACB, key_length));
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

                fprintf(results, "%s,%lu,%lu,%lu\n", "traditional", bitsA,
                        traditional->getStats().column_comparisons, duration);
                fprintf(stdout, "%s,%lu,%lu,%lu\n", "traditional", bitsA,
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