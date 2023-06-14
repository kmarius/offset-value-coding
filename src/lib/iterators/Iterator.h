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

    virtual ~Iterator() {
        assert(status == Closed);
    };

    /**
     * Open the operator. Must be called before calling next().
     */
    virtual void open() {
        assert(status == Unopened);
        status = Opened;
    };

    /**
     * Return the next Row, or nullptr, if this Iterator is isEmpty. open() must be called before this method.
     * free() must be called in between each call to next().
     * @return A pointer to the next row, or nullptr.
     */
    virtual Row *next() {
        assert(status == Opened);
        //assert((should_free = !should_free, should_free));
        return nullptr;
    };

    /**
     * Free the resources belonging to the row returned by the previous call to next(). Must be called before
     * calling next() again.
     */
    virtual void free() {
        assert(status == Opened);
        //assert((should_free = !should_free, !should_free));
    };

    /**
     * Close a previously openend Iterator. Calling next() or free() on a closed Iterator is illegal.
     */
    virtual void close() {
        assert(status == Opened);
        //assert(!should_free);
        status = Closed;
    };

    /**
     * Check if the output of this iterator is sorted.
     * @return true, if the output is sorted.
     */
    virtual bool outputIsSorted() {
        return false;
    }

    /**
     * Check if the output of this iterator has OVCs
     * @return true, if the output has OVCs
     */
    virtual bool outputHasOVC() {
        return false;
    }

    /**
     * Check if the output of this iterator is hashed.
     * @return true, if the output is hashed.
     */
    virtual bool outputIsHashed() {
        return false;
    }

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
    bool should_free = false;
};