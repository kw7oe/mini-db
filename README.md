## Simple SQLite in Rust

An attempt to write SQLite in Rust to learn about database
internals.

Currently, it is directly following the [Let's Build a Simple Database][0] tutorial.
The code is implemented up to Part 4 of the tutorial.

Once we completed the topic cover by the tutorial, we would try to extend
further as well.

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
