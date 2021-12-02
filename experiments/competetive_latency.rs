mod utils;

use postgres::NoTls;
use testcontainers::clients::{self};

use crate::utils::docker::{
    start_dockerized_postgres, start_pgbouncer, start_pgcloak, start_pgpool,
};

fn simple_query(connection_url: &str) {
    let mut client = postgres::Client::connect(connection_url, NoTls).unwrap();

    let rows = client.query("SELECT 0, 'Alex'", &[]).unwrap();

    let sum: &str = rows[0].get(1);
    assert_eq!(sum, "Alex");
}

fn main() {
    let docker = clients::Cli::default();

    let (database_connection_url, _postgres_node) = start_dockerized_postgres(&docker);

    // postgres 13.4 (no proxy)
    println!("no proxy");
    utils::benchmark::benchmark_function(&|| simple_query(&database_connection_url));

    // postgres 13.4 (pgcloak - session pooling - 10 max connections)
    println!("pgcloak");
    let (pgcloak_connection_url, _pgcloak_node, _pgcloak_tempdir) =
        start_pgcloak(&docker, &database_connection_url);

    utils::benchmark::with_docker_stats(_pgcloak_node.id(), &|| {
        utils::benchmark::benchmark_function(&|| simple_query(&pgcloak_connection_url));
    });
    drop(_pgcloak_node);
    drop(_pgcloak_tempdir);

    // postgres 13.4 (pg_pool)
    println!("pgpool");
    let (pgpool_connection_url, _pgpool_node) = start_pgpool(&docker, &database_connection_url);
    utils::benchmark::with_docker_stats(_pgpool_node.id(), &|| {
        utils::benchmark::benchmark_function(&|| simple_query(&pgpool_connection_url));
    });
    drop(_pgpool_node);

    // postgres 13.4 (pg_bouncer - session pooling - 10 max connections)
    println!("pgbouncer");
    let (pgbouncer_connection_url, _pgbouncer_node) =
        start_pgbouncer(&docker, &database_connection_url);
    utils::benchmark::with_docker_stats(_pgbouncer_node.id(), &|| {
        utils::benchmark::benchmark_function(&|| simple_query(&pgbouncer_connection_url));
    });
    drop(_pgbouncer_node);
}
