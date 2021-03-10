# Testing
## End-to-end tests
```
docker-compose up
cargo test --features e2e
```

# Useful commands cheat sheet

```
psql "sslmode=disable host=localhost port=5430 dbname=test"
```

# Thanks

Repos:

- [alex-dukhno/isomorphicdb](https://github.com/alex-dukhno/isomorphicdb) for a reference implementation of a postgres-compatible server in rust

Other:

- Jan Urbanski's slides from "Postgres on the wire" [Link](https://www.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf)
