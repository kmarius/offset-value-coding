#include "Shuffle.h"

#include <random>
#include <algorithm>

#define SHUFFLE_RUN_SIZE (PRIORITYQUEUE_CAPACITY * ((1 << RUN_IDX_BITS) - 3))
#define SHUFFLE_BUFFER_PAGES (1 << (RUN_IDX_BITS + 1))

namespace ovc::iterators {

    static inline std::string gen_path() {
        static int i = 0;
        return BASEDIR "/shuffle_" + std::to_string(i++) + ".dat";
    }

    Shuffle::Shuffle(Iterator *input) : UnaryIterator(input), buffer_manager(SHUFFLE_BUFFER_PAGES), count(0) {
    }

    Shuffle::~Shuffle() {
        for (auto r: runs) {
            r->remove();
            delete r;
        }
    }

    void Shuffle::open() {
        Iterator::open();
        auto rng = std::default_random_engine{};
        std::vector<Row> rows;
        std::vector<std::string> paths;
        rows.reserve(SHUFFLE_RUN_SIZE);

        input_->open();
        for (Row *row; (row = input_->next()); input_->free()) {
            if (rows.size() == SHUFFLE_RUN_SIZE) {
                std::shuffle(rows.begin(), rows.end(), rng);
                std::string path = gen_path();
                paths.push_back(path);
                ExternalRunW run = ExternalRunW(path, buffer_manager);
                for (auto &row1: rows) {
                    run.add(row1);
                }
                run.finalize();
                rows.clear();
            }
            rows.push_back(*row);
        }
        input_->close();

        if (!rows.empty()) {
            std::shuffle(rows.begin(), rows.end(), rng);
            std::string path = gen_path();
            paths.push_back(path);
            ExternalRunW run = ExternalRunW(path, buffer_manager);
            for (auto &row1: rows) {
                run.add(row1);
            }
            rows.clear();
            run.finalize();
        }

        for (auto &p: paths) {
            runs.push_back(new ExternalRunR(p, buffer_manager));
        }
    }

    void Shuffle::close() {
        Iterator::close();

        for (auto &r: runs) {
            r->remove();
            delete r;
        }
        runs.clear();
    }

    Row *Shuffle::next() {
        while (!runs.empty()) {
            int ind = rand() % runs.size();
            auto *run = runs[ind];
            Row *row = run->read();
            if (row) {
                count++;
                return row;
            }
            run->remove();
            delete run;
            runs.erase(runs.begin() + ind);
        }
        return nullptr;
    }
}