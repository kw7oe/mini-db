## Simple SQLite in Rust

An attempt to write SQLite in Rust to learn about database
internals.

It is implemented following the [Let's Build a Simple Database][0] tutorial.
The codebase now contain all the implementation in the tutorial.

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

[0]: https://cstack.github.io/db_tutorial/

## Testing

Since we are creating and removing file of the same name in our tests,
there might be race condition when we run our test concurrently that
cause our tests failed.

For the time being, we can avoid that by limiting the concurrency
in `cargo test`:

```sh
cargo test -- --test-threads=1
```
