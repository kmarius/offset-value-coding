#include "lib/Row.h"
#include "Shuffle.h"

#include <random>
#include <algorithm>

Shuffle::Shuffle(Iterator *_input) : input(_input), index(0), rows({}) {
};

Shuffle::~Shuffle() {
    assert(status == Closed);
};

void Shuffle::open() {
    assert(status == Unopened);
    status = Opened;

    input->open();
    rows = input->collect();
    auto rng = std::default_random_engine{};
    std::shuffle(std::begin(rows), std::end(rows), rng);
}

Row *Shuffle::next() {
    assert(status == Opened);
    if (index >= rows.size()) {
        return nullptr;
    }
    return &rows[index++];
}

void Shuffle::free() {}

void Shuffle::close() {
    assert(status == Opened);
    status = Closed;
    input->close();
}
