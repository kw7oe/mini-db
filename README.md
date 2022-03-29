## Simple SQLite in Rust

[![Rust](https://github.com/kw7oe/sqlite-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/kw7oe/sqlite-rust/actions/workflows/rust.yml)

An attempt to write SQLite in Rust to learn about database
internals.

It is implemented following the [Let's Build a Simple Database][0] tutorial.
The codebase now contain all the implementation in the tutorial.

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
- [ ] Implement delete operation for `sqlite`.
- [ ] Implement deletion for B+ Tree. _(this main contain multiple sub parts as well)_
- [ ] Implement update operation for `sqlite`.
- [ ] Implement buffer pool for our database. _([Reference][1])_
- [ ] Implement concurrency control for our database.
- [ ] Implement recovery mechanism for our database.
- [ ] Make it a distributed database????

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

## Testing

Since we are creating and removing file of the same name in our tests,
there might be race condition when we run our test concurrently that
cause our tests failed.

For the time being, we can avoid that by limiting the concurrency
in `cargo test`:

```sh
cargo test -- --test-threads=1
```

## References

- [Let's Build a Simple Database][0]
- [Database Internals](https://www.databass.dev/)
- [CMU 5-445/645 Intro to Database Systems (Fall 2019) Youtube Playlist](https://www.youtube.com/playlist?list=PLSE8ODhjZXjbohkNBWQs_otTrBTrjyohi)
- [CMU 15-445/645 (Fall 2021)](https://15445.courses.cs.cmu.edu/fall2021/)

[0]: https://cstack.github.io/db_tutorial/
[1]: https://15445.courses.cs.cmu.edu/fall2021/project1/
