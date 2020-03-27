# Database Management System Implementation

Here is the big lab project that I did during my Database Management System Implementation class during my master in Data Science at EURECOM (Télécom Paris).

## Goals
The main goals of this notebook are the following:
1. Implementation of three page replacement policies for buffer manager in a Minibase
2. Implementation of inequality joins

## Steps

### Task1 - Implementation of page replacement policies

One of the crucial task in database management is reading and writing pages from memory. Selection of subset of pages present in the memory is regarded as the buffer-pool. Buffer pool is organized into frames, where each frame holds a page from the disk. The buffer-manager is implemented to manage the allocation (pinning) and deallocation (unpinning) of pages from disk to the main memory and vice versa. Buffer manager maintains a pin counter, which stores the number of times a page is requested (but not released), and a dirty flag, which marks whether or not a page is updated. One important task of the buffer manager is to deploy a page replacement policy, when the buffer is full. Replacement policy is responsible for selecting the page to be flushed from the buffer-pool. Techniques like Least Recently Used [LRU], Mostly Recently Used [MRU], and Clock identify pages from the buffer-pool to be written to disk.

I will implement three different page replacement policies for buffer manager in Minibase, namely
1. First-In, First-Out [FIFO] is one of the elementary techniques used to carry-out page- replacement in the buffer. It is a low-overhead algorithm with little bookkeeping that uses a queue to maintain the order in which pages are accessed in memory. When a page is required to be replaced, the page in the front of the queue is selected to be flushed to disk.
2. Last-In, First-out [LIFO] is a technique that works similar to FIFO but the page to be replaced is selected from the back of the queue.
3. Least Recently Used - k Reference [LRU-K] is a technique that keeps track of the number of times each page is request in the last k-references to pages in buffer pool. The first two replacement policies are not discriminating against different pages whereas LRU-K discriminates between pages that are frequently and infrequently accessed in last k-references (k is a user supplied value). 

### Task2 - Inequality Join Assignment

In the previous phase of the project I implemented different page replacement policies to support buffer management.

For this phase of the project, I will experiment with a join operation. A join operation combines related tuples from same or different relations on different attributes schemes.
In particular, I will implement a novel operator for inequality joins and deliver a compara- tive analysis of different join implementations. Inequality joins have the following operators in the conjuncts of the join condition: < (less than), ≤ (less than or equal), ≥ (greater than or equal) and > (greater than).
