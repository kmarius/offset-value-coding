#pragma once

#include "Iterator.h"

// Generator interface

class IGenerator : public Iterator {

public:
    virtual ~IGenerator() {};

    virtual IGenerator *clone() const = 0;
};