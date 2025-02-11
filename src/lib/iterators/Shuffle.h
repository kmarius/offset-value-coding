#pragma once

#include "Iterator.h"
#include "lib/Partitioner.h"
#include "lib/io/ExternalRunR.h"

namespace ovc::iterators {

    class Shuffle : public UnaryIterator {
    public:
        explicit Shuffle(Iterator *input);

        ~Shuffle() override;

        void open() override;

        void close() override;

        Row *next() override;

        unsigned long getCount() const {
            return count;
        }

    private:
        BufferManager buffer_manager;
        std::vector<io::ExternalRunR *> runs;
        unsigned long count;
    };
}
