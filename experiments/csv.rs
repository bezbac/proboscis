mod utils;

use crate::utils::data::query_data_into_dataframe;
use crate::utils::docker::pgcloak::start_pgcloak;
use crate::utils::docker::pgcloak::ColumnConfiguration;
use crate::utils::docker::pgcloak::PgcloakConfig;
use crate::utils::docker::postgres::start_dockerized_postgres;
use crate::utils::fixtures::import_adult_data;
use polars::prelude::CsvWriter;
use polars::prelude::SerWriter;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::path::Path;
use testcontainers::clients::Cli;
use testcontainers::clients::{self};

fn query_to_csv(
    docker: &Cli,
    database_connection_url: &str,
    config: &PgcloakConfig,
    query: &str,
    filename: &str,
    offset: i64,
    length: usize,
) {
    let (pgcloak_connection_url, _pgcloak_node, _pgcloak_tempdir) =
        start_pgcloak(docker, database_connection_url, config);

    let result = query_data_into_dataframe(&pgcloak_connection_url, query).unwrap();

    let result = result.slice(offset, length);

    let path = Path::new("./target/output/experiments/");
    std::fs::create_dir_all(path).unwrap();
    let path = path.join(filename);

    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .append(true)
        .write(true)
        .open(path)
        .unwrap();

    let mut buf = BufWriter::new(file);

    CsvWriter::new(&mut buf)
        .with_delimiter(b';')
        .has_headers(true)
        .finish(&result)
        .expect("csv written");

    drop(_pgcloak_node);
    drop(_pgcloak_tempdir);
}

fn main() {
    let docker = clients::Cli::default();

    let (database_connection_url, _postgres_node) = start_dockerized_postgres(&docker);
    import_adult_data(&database_connection_url);

    let OFFSET = 180;
    let LENGTH = 10;

    // Original
    query_to_csv(
        &docker,
        &database_connection_url,
        &PgcloakConfig {
            columns: vec![],
            max_pool_size: 10,
            k: 0,
        },
        "SELECT * FROM adults",
        "ORIGINAL.csv",
        OFFSET,
        LENGTH,
    );

    // Select all
    query_to_csv(
        &docker,
        &database_connection_url,
        &PgcloakConfig {
            columns: vec![
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.workclass"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.sex"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.race"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.education"),
                },
            ],
            max_pool_size: 10,
            k: 50,
        },
        "SELECT * FROM adults",
        "ALL.csv",
        OFFSET,
        LENGTH,
    );

    // Subselection
    query_to_csv(
        &docker,
        &database_connection_url,
        &PgcloakConfig {
            columns: vec![
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.workclass"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.sex"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.race"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.education"),
                },
            ],
            max_pool_size: 10,
            k: 50,
        },
        "SELECT workclass, sex, relationship, class FROM adults",
        "SUBSELECTION.csv",
        OFFSET,
        LENGTH,
    );

    // Limit
    query_to_csv(
        &docker,
        &database_connection_url,
        &PgcloakConfig {
            columns: vec![
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.workclass"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.sex"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.race"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.education"),
                },
            ],
            max_pool_size: 10,
            k: 50,
        },
        "SELECT * FROM adults LIMIT 1000",
        "LIMIT.csv",
        OFFSET,
        LENGTH,
    );
}
