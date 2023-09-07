#pragma once

#include "Sort.h"

namespace ovc::iterators {
    typedef SortBase<true, true> SortDistinct;
    typedef SortBase<true, false> SortDistinctNoOvc;
}