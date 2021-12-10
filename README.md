<p align="center">
  <img src="resources/icon.png" alt="Logo" width="128" height="128">

  <h3 align="center">Proboscis</h3>

  <p align="center">
    An experimental, extensible, asynchronous PostgreSQL proxy
  </p>
</p>

# Development

## Testing

### End-to-end tests

```
docker-compose up
cargo test --features e2e
```

### Running pgcloak

```
docker-compose up
cargo run -p pgcloak -- -c pgcloak.example.toml -v DEBUG
```

The proxy should now be avaliable under `postgresql://admin:password@localhost:6432/postgres`
Example query:

```
psql -Atx postgresql://admin:password@localhost:6432/postgres -c 'SELECT 0'
```

#### Benchmarking using hyperfine

````
hyperfine --warmup 5 "psql -Atx postgresql://admin:password@localhost:6432/postgres?sslmode=disable -c 'SELECT 0'"
```

# Acknowledgements

Repos:

- [alex-dukhno/isomorphicdb](https://github.com/alex-dukhno/isomorphicdb) for a reference implementation of a postgres-compatible server in rust
- [sfackler/rust-postgres](https://github.com/sfackler/rust-postgres) for another implementation of the postgresql protocol in rust

Other:

- Jan Urbanski's slides from "Postgres on the wire" [Link](https://www.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf)
````
