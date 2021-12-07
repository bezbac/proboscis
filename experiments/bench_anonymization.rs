mod utils;

use crate::utils::data::query_data_into_dataframe;
use crate::utils::docker::start_dockerized_postgres;
use crate::utils::docker::start_pgcloak;
use postgres::{Client, NoTls};
use std::fs;
use testcontainers::clients::{self};

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

    drop(_pgcloak_node);
    drop(_pgcloak_tempdir);
}
