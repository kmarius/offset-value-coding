#pragma once

#include "Iterator.h"
#include "lib/io/BufferManager.h"
#include "lib/io/ExternalRunR.h"

class Scan : public Iterator {
public :
    explicit Scan(const std::string &path);

    ~Scan() override;

    void open() override;

    void close() override;

    Row *next() override;

    void free() override;

private :
    BufferManager buffer_manager;
    ExternalRunR run;
};