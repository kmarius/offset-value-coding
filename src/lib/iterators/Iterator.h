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
    public:

        Iterator() : status(Unopened),
                     output_has_ovc(false), output_is_hashed(false), output_is_sorted(false),
                     output_is_unique(false) {};

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
        bool outputIsSorted() const {
            return output_is_sorted;
        }

        /**
         * Check if the output of this iterator has OVCs
         * @return true, if the output has OVCs
         */
        bool outputHasOVC() const {
            return output_has_ovc;
        }

        /**
         * Check if the output of this iterator is hashed.
         * @return true, if the output is hashed.
         */
        bool outputIsHashed() const {
            return output_is_hashed;
        }

        /**
         * Check if the output of this iterator is unique.
         * @return true, if the output is unique.
         */
        bool outputIsUnique() const {
            return output_is_unique;
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
         * Consume all input_ and add it into a run in a file at the given path_sync.
         * Must be called on an unopened Iterator.
         * @param path The path_sync of the file.
         */
        void write(const std::string &path);
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

            // we can not copy this iterat because the destructor must only be called once
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

    protected:
        IteratorStatus status = Unopened;
        bool output_is_sorted;
        bool output_has_ovc;
        bool output_is_hashed;
        bool output_is_unique;
    };

    class Generator : public Iterator {
    public:
        ~Generator() override = default;

        virtual Generator *clone() const = 0;
    };

    class UnaryIterator : public Iterator {
    public:
        explicit UnaryIterator(Iterator *input) : input_(input) {}

        ~UnaryIterator() override {
            delete input_;
        }

        template<class T>
        T *getInput() {
            return reinterpret_cast<T *>(input_);
        }

    protected:
        Iterator *input_;
    };

    class BinaryIterator : public Iterator {
    public:
        BinaryIterator(Iterator *left, Iterator *right) : left(left), right(right) {}

        ~BinaryIterator() override {
            delete left;
            delete right;
        }

    protected:
        Iterator *left;
        Iterator *right;
    };
}