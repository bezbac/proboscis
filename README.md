<br />
<br />

<p align="center">
  <strong>🚨 Warning:</strong> the code contained in this repository is currently <br /> a proof of concept - it <i>should not be used in production</i></small>
</p>

<br />
<br />
<p align="center">
  <img src="resources/proboscis.png" alt="Proboscis Logo" width="128" height="128">

  <h3 align="center">Proboscis</h3>

  <p align="center">
    An experimental set of asynchronous, extensible <br /> building blocks for PostgreSQL compatible network applications.
  </p>
</p>
<br />
<br />

# Example

A basic postgesql proxy that can lookup query answers in a specified backend database.

```rust,no_run
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_connection_url = "postgresql://user:password@localhost:5432/database";

    let proxy_user = "admin";
    let proxy_password = "password";

    let mut proxy = Proxy::new(
        Config {
            credentials: hashmap! {
                proxy_user.to_string() => proxy_password.to_string(),
            },
            tls_config: None,
        },
        Box::new(
            PostgresResolver::create(
                TargetConfig::from_uri(database_connection_url).unwrap(),
                10,
            )
            .await
            .unwrap(),
        ),
    );

    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();

    proxy.listen(listener).await?;

    Ok(())
}
```

More examples can be found [here][examples].
For a larger "real world" example, have a look at the [pgcloak] source code.

[examples]: https://github.com/bezbac/proboscis/tree/main/examples
[pgcloak]: https://github.com/bezbac/proboscis/tree/main/crates/pgcloak

---

<br />
<br />
<p align="center">
  <img src="resources/pgcloak.png" alt="Pgcloak Logo" width="128" height="128">

  <h3 align="center">Pgcloak</h3>

  <p align="center">
    An experimental, privacy focused PostgreSQL network proxy.
  </p>
</p>
<br />
<br />

# Overview

#### Running pgcloak

```
docker-compose up
cargo run -p pgcloak -- -c pgcloak.example.toml -v DEBUG
```

The proxy should now be avaliable under postgresql://admin:password@localhost:6432/postgres

#### Building the pgcloak docker image

```
docker build -t pgcloak -f pgcloak.dockerfile .
```

#### Running the dockerized version

```
docker run -it --init --rm -v $PWD:/app -p 6432:6432 pgcloak -v debug -c pgcloak.example.toml
```

# Experiments

**🚧 Note:**  
The experiments have only been tested on MacOS 11.6 using Docker Desktop 4.0.0.  
They might not work as expected when run on a different operating system or with another version of Docker.

#### Running the experiments without analysis

```
cargo run --example competetive_latency
```

#### Running the experiments with analyis

**🚧 Note: This requires python3 with matplotlib, numpy & seaborn to be installed**

```
cargo +nightly run --features analysis --example competetive_latency
```

<br />
<br />

---

# Acknowledgements

Repos:

- [alex-dukhno/isomorphicdb](https://github.com/alex-dukhno/isomorphicdb) for a reference implementation of a postgres-compatible server in rust
- [sfackler/rust-postgres](https://github.com/sfackler/rust-postgres) for an implementation of the postgresql network protocol and a compatible client in rust

Other:

- Jan Urbanski's slides from "Postgres on the wire" [Link](https://www.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf)
