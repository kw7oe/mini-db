## Mini DBMS in Rust

[![Rust](https://github.com/kw7oe/sqlite-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/kw7oe/sqlite-rust/actions/workflows/rust.yml)

An attempt to write database management system (DBMS) in Rust
to learn about database internals.

We started by implementing it following the [Let's Build a Simple Database][0]
tutorial _(which is based on SQLite)_. However, as we continue the work after the
tutorial, we kind of diverted from following the SQLite implementation and specification.
Hence, it's not entirely correct to call this SQLite in Rust anymore.

The rest of the work is continue based on multiple [references](#references).
One of the most important one is [CMU Intro to Database Systems projects][5]
_(including previous years archive, since each year they have students
implement different structure)_.

While our B+ tree now support concurrent operations, it's still a single
threaded database system, as our frontend (network/cli layer) doesn't support
handling concurrent requests yet.

_This is by no mean an idiomatic Rust implementation as I'm learning Rust
along the way._

## End Goal

The main focus to write a storage engine from scratch. This project now
includes it's own B+ Tree data structure, buffer pool, LRU replacement policy,
transaction manager, and lock manager.

What's next is to implement the recovery system by implement a log manager for
our Write Ahead Log (WAL) and ARIES protocol.

## Non Goals

We won't go deep into the query engine. Currently, we have two variance of the
query engine, one is written during the early phase where it just parse string
and execute functions directly. The newer implementation includes a simplify
query engine with query plan and query executor.

We won't write our SQL query parser, query rewriter and optimizer for the time
being.

We would just write a simple enough query engine so we can test out our
implementation.

## Progress

Since we have completed through the tutorial of Let's Build a Simple Database,
we will have to come up with our own checklist for this project. Here's a
quick breakdown of my journey and what I'm going to implement next:

- [x] Add test case for splitting node and updating parent, where the new node is not the most right child.
- [x] Implement split on internal node.
- [x] Extend test cases to make sure insertion is working as intended and fix
      bugs along the way...
  - [x] Add property-based testing for insertion test.
  - [x] Fix all the bugs found through property based test.
- [x] Implement find by id operation. This would be helpful when testing
      deletion.
- [x] Implement delete operation for `sqlite`.
- [x] Implement deletion for B+ Tree.
  - [x] Implement deletion on leaf node.
  - [x] Implement deletion on internal node.
  - [x] Implement merging after the neighbouring nodes pointers/key-values < N.
  - [x] Implement mergeing for internal nodes.
  - [ ] Implement rebalancing to reduce the number of merges need.
    - [x] Implement steal from siblings if can't merge both nodes together.
    - [ ] Check if there's still other rebalancing optimisation technique
      available...
  - [ ] Replace hardcoded max internal node count of 3 with the actual internal
    node count supported by our data format.
      - This require us to generate a larger datasets to tests the
        behaviour.
- [x] Implement buffer pool for our database. _([Reference][1])_
  - [x] Implement least recently used (LRU) replacement policies.
  - [x] Implement Buffer Pool Manager.
  - [x] Integrated Buffer Pool manager into the rest of the system.
  - [x] Make buffer pool page size configurable.
- [x] Multi threaded index concurrency control. _([Reference][2], [Reference, see Task 4][3])_
    - [x] Support concurrent insert to B+ Tree.
    - [x] Support concurrent select to B+ Tree.
    - [x] Support concurrent delete to B+ Tree.
    - [x] Test concurrent insert + select;
    - [x] Test concurrent delete + select;
    - [x] Test concurrent insert + delete;
    - [x] Test concurrent insert + select + delete;
    - [ ] Optimize latch crabbing by holding read lock and only swap to write
      lock when there's a split/merge.
    - [ ] Refactor tree operation out of `Pager`.
- [ ] Implement concurrency control at row/tuple level. _([Reference][4])_
  - [x] Implement a transaction manager first.
  - [x] Implement lock manager.
    - It currently doesn't support different locking behaviour based on
      different isolation levels.
    - While is completed, I can't really guarantee the correctness of the
      implementation for the time being. See the comments in code for more.
    - Testing for read and write anomalies are not included yet. This will
    require implementation of update operation and integration of lock manager
    into the query/table code, which is what I plan to do next.
  - [x] Implement a query executor.
    - Implemented both sequence scan and delete executor and plan node.
    - It is not integrated into the other part of the systems yet.
  - [x] Support update operation. This is important as it allow us to produce
  test case that can lead to read/write anomalies.
    - [x] Implement update plan node.
    - [x] Implement update executor.
    - [x] Support usage of index scan in update plan node.
  - [x] Write test to ensure that two phase locking works on all read and write
    anomolies.
    - [x] Update query executor, table to ensure lock is acquired correctly so the
      tests for read/write anomolies passed.
    - Currently, the test is only available for index scan executor. To
      ensure, we can test the read/write anomolies with sequence scan, we need
      to first support where expression evaluation in our query engine.
      Which would be a task for another day.
  - [ ] Implement concurrent query execution.
  - [ ] Implement dead lock detection.
  - [ ] Implement dead lock prevention. (Wound Wait algorithm)
    _([Reference](https://15445.courses.cs.cmu.edu/fall2021/project4/#deadlock_prevention))_
- [ ] Implement recovery mechanism for our database. ([Reference][6])
  - [ ] Implement WAL.
  - [ ] Implement ARIES.
- [ ] Update our query parser to integrate with our new query executor. This will allow us to
  easily test things by using SQL statement instead of manually writing our query plan.
  - [ ] Implement insert executor.
  - [ ] Parse query into query plan.
  - [ ] Replace the query engine with the new onw in `main.rs`.

_(subject to changes as we progress)_

## References

- [Let's Build a Simple Database][0]
- [Database Internals](https://www.databass.dev/)
- [CMU 5-445/645 Intro to Database Systems (Fall 2019) Youtube Playlist](https://www.youtube.com/playlist?list=PLSE8ODhjZXjbohkNBWQs_otTrBTrjyohi)
- [CMU 15-445/645 (Fall 2021)](https://15445.courses.cs.cmu.edu/fall2021/)

[0]: https://cstack.github.io/db_tutorial/
[1]: https://15445.courses.cs.cmu.edu/fall2021/project1/
[2]: https://www.youtube.com/watch?v=x5tqzyf0zrk&list=PLSE8ODhjZXjbohkNBWQs_otTrBTrjyohi&index=9
[3]: https://15445.courses.cs.cmu.edu/fall2020/project2/
[4]: https://15445.courses.cs.cmu.edu/fall2020/project4/
[5]: https://15445.courses.cs.cmu.edu/fall2021/assignments.html
[6]: https://15445.courses.cs.cmu.edu/fall2018/project4/
