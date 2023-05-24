#pragma once

#include <cstdint>

/*
 * For O_DIRECT, buffer must be 512-byte aligned.
 */

#define BUFFER_SIZE 4096
#define BUFFER_ALIGNMENT 512

struct alignas(BUFFER_ALIGNMENT) Buffer {
    uint8_t data[BUFFER_SIZE];
};