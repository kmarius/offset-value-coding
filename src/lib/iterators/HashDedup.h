#pragma once

#include "Iterator.h"
#include "lib/HashSet.h"

class HashDedup : public Iterator {
public :
    explicit HashDedup(Iterator *input);

    ~HashDedup() override;

    void open() override;

    void close() override;

    Row *next() override;

    bool outputIsHashed() override {
        return true;
    };

private :
    Iterator *input_;
    HashSet hashSet;
};
