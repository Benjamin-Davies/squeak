# Squeak

An experiment to read (and maybe someday write) SQLite3 databases using idiomatic Rust.

## Roadmap

- [x] Read tables
- [x] Read indices
- [x] Derive macro for `Table` trait
- [ ] Write tables
- [ ] Write indices
- [ ] Transactions
- [ ] LRU page cache

### Non-goals

- SQL support, except maybe for schema validation

## References

https://www.sqlite.org/fileformat.html
