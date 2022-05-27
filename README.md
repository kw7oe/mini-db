## Mini RDBMS in Rust

[![Rust](https://github.com/kw7oe/sqlite-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/kw7oe/sqlite-rust/actions/workflows/rust.yml)

An attempt to write ~~SQLite~~ relational database management system (RDBMS) in Rust
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
threaded database system.

_This is by no mean an idiomatic Rust implementation as I'm learning Rust
along the way._

## What's Next?

Since we have completed through the tutorial of Let's Build a Simple Database,
we will have to come up with our own checklist for this project. Here's a
quick breakdown on what I'm going to implement next:

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
- [ ] Implement concurrency control at row/tuple level. _([Reference][4])_
  - [ ] Implement a transaction manager first.
  - [ ] Implement lock manager.
  - [ ] Implement dead lock detection.
  - [ ] Implement concurrent query execution.
- [ ] Implement recovery mechanism for our database.
  - [ ] Implement WAL.
  - [ ] Implement ARIES.

_(subject to changes as we progress)_

## End Goal

The main focus is writing a storage engine from scratch. This means
writing our own B+ Tree data structure, buffer pool and
maybe WAL as well.

If possible, we would also be writing the transaction, recovery
and lock manager for our storage engine.

## Non Goals

We won't go deep into the query parser and optimizer, as well
as our execution engine.

We would just write a simple enough query and execution engine
so we can test out our database.

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
