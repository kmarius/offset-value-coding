#pragma once

#include "Iterator.h"
#include "lib/io/BufferManager.h"
#include "lib/io/ExternalRunR.h"

class Scan : public Iterator {
public :
    explicit Scan(const std::string &path);

    Row *next() override;

private :
    BufferManager buffer_manager;
    ExternalRunR run;
};