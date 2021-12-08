mod utils;

use crate::utils::benchmark::print_benchmark_stats;
use crate::utils::data::query_data_into_dataframe;
use crate::utils::docker::pgcloak::start_pgcloak;
use crate::utils::docker::pgcloak::ColumnConfiguration;
use crate::utils::docker::pgcloak::PgcloakConfig;
use crate::utils::docker::postgres::start_dockerized_postgres;
use postgres::{Client, NoTls};
use std::fs;
use testcontainers::clients::{self};

const QUERY: &str = "SELECT age, sex, race, workclass, education FROM adults";

fn seed_database(database_connection_url: &str) {
    let mut client = Client::connect(database_connection_url, NoTls).unwrap();
    let adults_data = "./experiments/resources/adults.sql";
    let contents = fs::read_to_string(adults_data).expect("Something went wrong reading the file");
    client.batch_execute(&contents).unwrap();
}

fn benchmark(docker: &clients::Cli, database_connection_url: &str, config: &PgcloakConfig) {
    let (pgcloak_connection_url, _pgcloak_node, _pgcloak_tempdir) =
        start_pgcloak(docker, database_connection_url, config);

    let result = query_data_into_dataframe(&pgcloak_connection_url, QUERY);
    println!("{}", result.head(Some(12)));

    let (docker_stats, baseline_durations) =
        utils::docker::stats::while_collecting_docker_stats(_pgcloak_node.id(), &|| {
            utils::benchmark::benchmark_function(20, &|| {
                let mut client = Client::connect(&pgcloak_connection_url, NoTls).unwrap();
                client.query(QUERY, &[]).unwrap();
            })
        });
    print_benchmark_stats(&baseline_durations);

    drop(_pgcloak_node);
    drop(_pgcloak_tempdir);
}

fn main() {
    let docker = clients::Cli::default();

    let (database_connection_url, _postgres_node) = start_dockerized_postgres(&docker);

    seed_database(&database_connection_url);

    let result = query_data_into_dataframe(&database_connection_url, QUERY);
    println!("{}", result.head(Some(12)));

    println!("pgcloak baseline");
    benchmark(
        &docker,
        &database_connection_url,
        &PgcloakConfig {
            k: 3,
            columns: vec![],
            max_pool_size: 10,
        },
    );

    println!("pgcloak k-anonymization | k=3 | QI columns: age, sex, race, workclass, education");
    benchmark(
        &docker,
        &database_connection_url,
        &PgcloakConfig {
            columns: vec![
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.age"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.sex"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.race"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.workclass"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.education"),
                },
            ],
            max_pool_size: 10,
            k: 3,
        },
    );

    println!("pgcloak k-anonymization | k=10 | QI columns: age, sex, race, workclass, education");
    benchmark(
        &docker,
        &database_connection_url,
        &PgcloakConfig {
            columns: vec![
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.age"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.sex"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.race"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.workclass"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.education"),
                },
            ],
            max_pool_size: 10,
            k: 10,
        },
    );

    println!("pgcloak k-anonymization | k=30 | QI columns: age, sex, race, workclass, education");
    benchmark(
        &docker,
        &database_connection_url,
        &PgcloakConfig {
            columns: vec![
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.age"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.sex"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.race"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.workclass"),
                },
                ColumnConfiguration::PseudoIdentifier {
                    name: String::from("adults.education"),
                },
            ],
            max_pool_size: 10,
            k: 10,
        },
    );
}
