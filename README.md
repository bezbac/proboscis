<p align="center">
  <img src="resources/icon.png" alt="Logo" width="128" height="128">

  <h3 align="center">Proboscis</h3>

  <p align="center">
    An experimental, extensible PostgreSQL proxy
  </p>
</p>

# Development

## Testing

### End-to-end tests

```
docker-compose up
cargo test --features e2e
```

# Acknowledgements

Repos:

- [alex-dukhno/isomorphicdb](https://github.com/alex-dukhno/isomorphicdb) for a reference implementation of a postgres-compatible server in rust
- [sfackler/rust-postgres](https://github.com/sfackler/rust-postgres) for another implementation of the postgresql protocol in rust

Other:

- Jan Urbanski's slides from "Postgres on the wire" [Link](https://www.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf)
