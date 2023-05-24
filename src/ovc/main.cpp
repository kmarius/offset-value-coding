#include "lib/iterators/Filter.h"
#include "lib/iterators/Generator.h"
#include "lib/iterators/Sort.h"
#include "lib/PriorityQueue.h"
#include "lib/log.h"
#include "lib/iterators/AssertSorted.h"

#include <vector>
#include "lib/utils.h"

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

void example_run_generation() {
    // three "memory_runs" are reserved for: low fences, high fences, and the we-are-merging-indicator
    size_t num_rows = QUEUE_CAPACITY * QUEUE_CAPACITY * (QUEUE_CAPACITY - 3) * 17;

    log_info("num_rows=%lu", num_rows);

    Iterator *plan = new AssertSorted(
            new Sort(
                    new Generator(num_rows, DOMAIN, 7)
            )
    );

    plan->run();

    delete plan;
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

int main(int argc, char *argv[]) {
    log_open(LOG_TRACE);
    // log_set_quiet(true);
    log_set_level(LOG_INFO);
    log_info("start", "");

    auto start = now();

    example_run_generation();

    log_info("elapsed=%lums", since(start));

    log_info("fin");
    log_close();
    return 0;
}