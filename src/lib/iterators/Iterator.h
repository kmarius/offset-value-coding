#pragma once

#include "lib/defs.h"
#include "lib/Row.h"

#include <vector>

typedef enum IteratorStatus {
    Unopened,
    Opened,
    Closed,
} IteratorStatus;

class Iterator {
public:
    Iterator();

    virtual ~Iterator();

    /**
     * Open the operator. Must be called before calling next().
     */
    virtual void open() = 0;

    /**
     * Return the next Row, or nullptr, if this Iterator is isEmpty. open() must be called before this method.
     * free() must be called in between each call to next().
     * @return A pointer to the next row, or nullptr.
     */
    virtual Row *next() = 0;

    /**
     * Free the resources belonging to the row returned by the previous call to next(). Must be called before
     * calling next() again.
     */
    virtual void free() = 0;

    /**
     * Close a previously openend Iterator. Calling next() or free() on a closed Iterator is illegal.
     */
    virtual void close() = 0;

    /**
     * Run the iterator and optionally print all rows.
     * @param print true if output should be printed
     */
    void run(bool print = false);

    /**
     * Return a vector of all rows. Must be called on an unopened Iterator.
     * @return The vector of rows.
     */
    std::vector<Row> collect();

    /**
     * Consume all input and add it into a run in a file at the given path_sync.
     * Must be called on an unopened Iterator.
     * @param path The path_sync of the file.
     */
    void write(const std::string &path);

protected:
    IteratorStatus status = Unopened;
};