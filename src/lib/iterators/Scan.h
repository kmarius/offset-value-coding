#pragma once

#include "Iterator.h"
#include "lib/io/BufferManager.h"
#include "lib/io/ExternalRunR.h"

namespace ovc::iterators {
    using namespace ovc::io;

    class Scan : public Iterator {
    public :
        explicit Scan(const std::string &path) : buffer_manager(1), run(path, buffer_manager) {};

        Row *next() override {
            Iterator::next();
            return run.read();
        };

    private :
        BufferManager buffer_manager;
        ExternalRunR run;
    };
}