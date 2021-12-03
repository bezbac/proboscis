mod utils;

use crate::utils::benchmark::print_benchmark_stats;
use crate::utils::docker::start_dockerized_postgres;
use crate::utils::docker::start_pgcloak;
use arrow::record_batch::RecordBatch;
use polars::frame::DataFrame;
use postgres::{Client, NoTls};
use std::convert::TryFrom;
use std::fs;
use testcontainers::clients::{self};

fn query_data_into_dataframe(
    postgres_connection_string: &str,
    queries: &[&str],
) -> polars::frame::DataFrame {
    // TODO: IMPLEMENT THIS
    unimplemented!();
}

fn main() {
    let docker = clients::Cli::default();

    let (database_connection_url, _postgres_node) = start_dockerized_postgres(&docker);

    let mut client = Client::connect(&database_connection_url, NoTls).unwrap();

    let adults_data = "./experiments/resources/adults.sql";
    let contents = fs::read_to_string(adults_data).expect("Something went wrong reading the file");
    client.batch_execute(&contents).unwrap();

    let result = query_data_into_dataframe(&database_connection_url, &["SELECT * FROM adults"]);
    println!("{}", result.head(Some(12)));

    // PGCLOAK: Example
    println!("pgcloak");
    let (pgcloak_connection_url, _pgcloak_node, _pgcloak_tempdir) =
        start_pgcloak(&docker, &database_connection_url);

    let result = query_data_into_dataframe(&pgcloak_connection_url, &["SELECT * FROM adults"]);
    println!("{}", result.head(Some(12)));

    // PGCLOAK: Benchmark
    let (docker_stats, pgcloak_durations) =
        utils::docker::while_collecting_docker_stats(_pgcloak_node.id(), &|| {
            utils::benchmark::benchmark_function(100, &|| {
                let result =
                    query_data_into_dataframe(&pgcloak_connection_url, &["SELECT * FROM adults"]);
            })
        });
    print_benchmark_stats(&pgcloak_durations);

    drop(_pgcloak_node);
    drop(_pgcloak_tempdir);
}
