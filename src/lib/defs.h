#pragma once

// Use O_DIRECT to add through operating system caches
#define USE_O_DIRECT

// Use synchronuous IO (TODO)
//#define USE_SYNC_IO

// Where should memory_runs be stored on disk?
// Memory (doesn't support O_DIRECT)
//#define BASEDIR "/tmp"

// TODO: create BASEDIR if it doesn't exist?
// SSD:
#define BASEDIR "/home/marius"

// HDD:
//#define BASEDIR "/home/marius/Data"

// This currently controls how many bits we use to store the run index in the key in the priority queue
// which at the same time controls the fan-in of the merge
// TODO: we should decouple this from the queue size
#define RUN_IDX_BITS 8

#define PRIORITYQUEUE_CAPACITY (1 << 10)

#define LOGPATH "/tmp/ovc.log"
//#define NO_LOGGING

#define likely(x) __builtin_expect(x, 1)
#define unlikely(x) __builtin_expect(x, 0)

typedef unsigned long Count;
typedef unsigned long OVC;
typedef unsigned long Index;