#include "lib/iterators/Filter.h"
#include "lib/iterators/Generator.h"
#include "lib/iterators/Sort.h"
#include "lib/PriorityQueue.h"
#include "lib/log.h"
#include "lib/iterators/AssertSorted.h"

#include <vector>
#include "lib/utils.h"
#include "lib/Schema.h"
#include "lib/iterators/Dedup.h"
#include "lib/iterators/AssertSortedUnique.h"

bool pred(Row *row) {
    return row->columns[0] & 1;
}

void example_simple_tree() {
    Iterator *plan = new Filter(
            pred,
            new Generator(10000, 1000)
    );
    plan->run(true);
    delete plan;
}

void example_dedup() {
    size_t num_rows = QUEUE_CAPACITY * QUEUE_CAPACITY * (QUEUE_CAPACITY - 3) * 4;

    log_info("num_rows=%lu", num_rows);

    auto dedup = new Dedup(
                    new Sort(
                            new Generator(num_rows, DOMAIN, 1337)
                    ));
    Iterator *plan = new AssertSortedUnique(dedup);
    plan->run();

    log_info("num_dupex: %d", dedup->num_dupes);

    delete plan;

    log_info("nlogn:                   %lu", num_rows * (size_t) log((double) num_rows));
    log_info("comparisons:             %lu", stats.comparisons);
    log_info("full_comparisons:        %lu", stats.full_comparisons);
    log_info("actual_full_comparisons: %lu", stats.actual_full_comparisons);
}

void example_sort() {
    // three "memory_runs" are reserved for: low fences, high fences, and the we-are-merging-indicator
    //size_t num_rows = QUEUE_CAPACITY * QUEUE_CAPACITY * (QUEUE_CAPACITY - 3) * 4;
    size_t num_rows = 2;

    log_info("num_rows=%lu", num_rows);

    Iterator *plan =
            new Sort(
                    new Generator(num_rows, DOMAIN, 1337)
            );



    plan->run(true);

    delete plan;

    log_info("nlogn:                   %lu", num_rows * (size_t) log((double) num_rows));
    log_info("comparisons:             %lu", stats.comparisons);
    log_info("full_comparisons:        %lu", stats.full_comparisons);
    log_info("actual_full_comparisons: %lu", stats.actual_full_comparisons);
}

void example_empty_run() {
    Iterator *plan = new AssertSorted(
            new Sort(
                    new Generator(0, DOMAIN)
            )
    );
    plan->run();
    delete plan;
}

void raw_rows() {
    Schema schema(4, INTEGER, INTEGER, CHAR, VARCHAR + 17);
    log_info("%s", schema.to_string().c_str());

    uint8_t buf[1024];
    //auto row = (RawRow *) buf;
    RawRow *a = create(&schema, 1, 2, 'a', "hey dude");

    log_info("col 0: %d", a->get<int>(0, schema));
    log_info("col 3: %s", &a->get<char>(3, schema));

    log_info("%s", a->to_string(schema).c_str());

    delete a;
}

int main(int argc, char *argv[]) {
    log_open(LOG_TRACE);
    // log_set_quiet(true);
    log_set_level(LOG_INFO);
    log_info("start", "");

    auto start = now();

    //raw_rows();
    example_sort();
    //example_dedup();

    log_info("elapsed=%lums", since(start));

    log_info("fin");
    log_close();
    return 0;
}