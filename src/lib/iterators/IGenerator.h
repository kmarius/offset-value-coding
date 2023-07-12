#pragma once

#include "Iterator.h"

// Generator interface

namespace ovc::iterators {

    class IGenerator : public Iterator {

    public:
        virtual ~IGenerator() {};

        virtual IGenerator *clone() const = 0;
    };
}