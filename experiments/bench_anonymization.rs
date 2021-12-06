mod utils;

use crate::utils::benchmark::print_benchmark_stats;
use crate::utils::docker::start_dockerized_postgres;
use crate::utils::docker::start_pgcloak;
use polars::frame::DataFrame;
use polars::prelude::NamedFrom;
use polars::prelude::Series;
use postgres::types::Type;
use postgres::{Client, NoTls};
use std::fs;
use testcontainers::clients::{self};

fn query_data_into_dataframe(
    postgres_connection_string: &str,
    query: &str,
) -> polars::frame::DataFrame {
    let mut client = Client::connect(&postgres_connection_string, NoTls).unwrap();

    let rows = client.query(query, &[]).unwrap();

    let series = rows
        .first()
        .unwrap()
        .columns()
        .iter()
        .enumerate()
        .map(|(i, column)| match column.type_() {
            &Type::BOOL => Series::new(
                column.name(),
                rows.iter()
                    .map(|row| row.get::<usize, bool>(i))
                    .collect::<Vec<bool>>(),
            ),
            &Type::INT2 => Series::new(
                column.name(),
                rows.iter()
                    .map(|row| row.get::<usize, i16>(i))
                    .collect::<Vec<i16>>(),
            ),
            &Type::INT4 => Series::new(
                column.name(),
                rows.iter()
                    .map(|row| row.get::<usize, i32>(i))
                    .collect::<Vec<i32>>(),
            ),
            &Type::OID => Series::new(
                column.name(),
                rows.iter()
                    .map(|row| row.get::<usize, u32>(i))
                    .collect::<Vec<u32>>(),
            ),
            &Type::CHAR => Series::new(
                column.name(),
                rows.iter()
                    .map(|row| row.get::<usize, i8>(i))
                    .collect::<Vec<i8>>(),
            ),
            &Type::TEXT => Series::new(
                column.name(),
                rows.iter()
                    .map(|row| row.get::<usize, &str>(i))
                    .collect::<Vec<&str>>(),
            ),
            _ => unimplemented!("{:?}", column.type_()),
        })
        .collect();

    DataFrame::new(series).unwrap()
}

fn seed_database(database_connection_url: &str) {
    let mut client = Client::connect(database_connection_url, NoTls).unwrap();
    let adults_data = "./experiments/resources/adults.sql";
    let contents = fs::read_to_string(adults_data).expect("Something went wrong reading the file");
    client.batch_execute(&contents).unwrap();
}

fn main() {
    let docker = clients::Cli::default();

    let (database_connection_url, _postgres_node) = start_dockerized_postgres(&docker);

    seed_database(&database_connection_url);

    let result = query_data_into_dataframe(&database_connection_url, "SELECT * FROM adults");
    println!("{}", result.head(Some(12)));

    // PGCLOAK: Example
    println!("pgcloak");
    let (pgcloak_connection_url, _pgcloak_node, _pgcloak_tempdir) =
        start_pgcloak(&docker, &database_connection_url);

    let result = query_data_into_dataframe(&pgcloak_connection_url, "SELECT * FROM adults");
    println!("{}", result.head(Some(12)));

    // PGCLOAK: Benchmark
    let (docker_stats, pgcloak_durations) =
        utils::docker::while_collecting_docker_stats(_pgcloak_node.id(), &|| {
            utils::benchmark::benchmark_function(100, &|| {
                let result =
                    query_data_into_dataframe(&pgcloak_connection_url, "SELECT * FROM adults");
            })
        });
    print_benchmark_stats(&pgcloak_durations);

    drop(_pgcloak_node);
    drop(_pgcloak_tempdir);
}
