#pragma once

#include "lib/defs.h"
#include "lib/Row.h"

#include <vector>
#include <iostream>

namespace ovc::iterators {

    typedef enum IteratorStatus {
        Unopened,
        Opened,
        Closed,
    } IteratorStatus;

    class Iterator {
    protected:
        iterator_stats stats;
        IteratorStatus status = Unopened;
    public:

        Iterator() : status(Unopened), stats() {};

        virtual ~Iterator() {
            assert(status != Opened);
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

        iterator_stats &getStats() {
            return stats;
        }

        virtual void accumulateStats(struct iterator_stats &acc) {
            acc.add(stats);
        }

        struct iter {
            using iterator_category = std::input_iterator_tag;
            using difference_type = std::ptrdiff_t;
            using value_type = Row;
            using pointer = value_type *;
            using reference = value_type &;

            explicit iter(Iterator *self) : self(self), row(nullptr) {
                if (self) {
                    if (self->status == Unopened) {
                        self->open();
                    }
                    row = self->next();
                }
            }

            // we can not copy this iter because the destructor must only be called once
            iter(const iter &it) = delete;

            ~iter() {
                if (self && self->status == Opened) {
                    self->close();
                }
            }

            reference operator*() const { return *row; }

            pointer operator->() { return row; }

            iter &operator++() {
                if (self) {
                    if (row) {
                        self->free();
                    }
                    row = self->next();
                }
                return *this;
            }

            Row *operator++(int) {
                Row *tmp = row;
                ++(*this);
                return tmp;
            }

            friend bool operator==(const iter &a, const iter &b) { return a.row == b.row; };

            friend bool operator!=(const iter &a, const iter &b) { return !(a == b); };

        private:
            Iterator *self;
            Row *row;
        };

        iter begin() {
            return iter(this);
        }

        static iter end() {
            return iter(nullptr);
        }

    };

    class Generator : public Iterator {
    public:
        ~Generator() override = default;

        virtual Generator *clone() const = 0;
    };

    class UnaryIterator : public Iterator {
    public:
        explicit UnaryIterator(Iterator *input) : input(input) {}

        ~UnaryIterator() override {
            delete input;
        }

        template<class T = Iterator>
        T *getInput() {
            return reinterpret_cast<T *>(input);
        }

        void accumulateStats(iterator_stats &acc) override {
            input->accumulateStats(acc);
            acc.add(stats);
        }

    protected:
        Iterator *input;
    };

    class BinaryIterator : public Iterator {
    public:
        BinaryIterator(Iterator *left, Iterator *right) : left(left), right(right) {}

        ~BinaryIterator() override {
            delete left;
            delete right;
        }

        template<class T>
        T *getLeftInput() {
            return reinterpret_cast<T *>(left);
        }

        template<class T>
        T *getRightInput() {
            return reinterpret_cast<T *>(right);
        }

        void accumulateStats(iterator_stats &acc) override {
            left->accumulateStats(acc);
            right->accumulateStats(acc);
            acc.add(stats);
        }

    protected:
        Iterator *left;
        Iterator *right;
    };
}