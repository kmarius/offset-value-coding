#pragma once

#include "Sort.h"

namespace ovc::iterators {
    typedef SortBase<DistinctOn, OvcOn> InSortDistinct;
    typedef SortBase<DistinctOn, OvcOff> InSortDistinctNoOvc;
}