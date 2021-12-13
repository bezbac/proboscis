mod utils;

use crate::utils::benchmark::print_benchmark_stats;
use crate::utils::data::query_data_into_dataframe;
use crate::utils::docker::pgcloak::start_pgcloak;
use crate::utils::docker::pgcloak::ColumnConfiguration;
use crate::utils::docker::pgcloak::PgcloakConfig;
use crate::utils::docker::postgres::start_dockerized_postgres;
use arrow::datatypes::Float64Type;
use ndarray_stats::EntropyExt;
use polars::prelude::DataFrame;
use postgres::{Client, NoTls};
use std::fs;
use std::time::Instant;
use testcontainers::clients::{self};

const QUERY: &str = "SELECT * FROM adults";

fn seed_database(database_connection_url: &str) {
    let mut client = Client::connect(database_connection_url, NoTls).unwrap();
    let adults_data = "./experiments/resources/adults.sql";
    let contents = fs::read_to_string(adults_data).expect("Something went wrong reading the file");
    client.batch_execute(&contents).unwrap();
}

fn count_equivalence_classes(df: &DataFrame, columns: &[&str]) -> usize {
    let df = df.select(columns.to_vec()).unwrap();
    let df = df.drop_duplicates(true, None).unwrap();
    df.height()
}

fn benchmark(
    docker: &clients::Cli,
    database_connection_url: &str,
    config: &PgcloakConfig,
    result_qi_columns: &[String],
    baseline: &DataFrame,
) {
    let (pgcloak_connection_url, _pgcloak_node, _pgcloak_tempdir) =
        start_pgcloak(docker, database_connection_url, config);

    let before = Instant::now();
    let result = query_data_into_dataframe(&pgcloak_connection_url, QUERY);
    let after = Instant::now();

    // println!("{}", result.head(Some(12)));

    // let (docker_stats, baseline_durations) =
    //     utils::docker::stats::while_collecting_docker_stats(_pgcloak_node.id(), &|| {
    //         utils::benchmark::benchmark_function(20, &|| {
    //             let mut client = Client::connect(&pgcloak_connection_url, NoTls).unwrap();
    //             client.query(QUERY, &[]).unwrap();
    //         })
    //     });
    // print_benchmark_stats(&baseline_durations);

    drop(_pgcloak_node);
    drop(_pgcloak_tempdir);

    let query_duration = after.duration_since(before).as_millis();
    println!("Query duration {}ms", query_duration);

    if result_qi_columns.len() > 0 {
        let equivalence_class_count = count_equivalence_classes(
            &result,
            &result_qi_columns
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<&str>>(),
        );
        println!("Equivalence class count {:?}", equivalence_class_count);

        let average_eq_size = result.height() / equivalence_class_count;
        println!("Average eq size {:?}", average_eq_size);

        let normalized_average_eq_size = average_eq_size / config.k;
        println!(
            "Normalized average eq size {:?}",
            normalized_average_eq_size
        );
    }

    // let baseline_ndarray = baseline.to_ndarray::<Float64Type>().unwrap();
    // let ndarray = result.to_ndarray::<Float64Type>().unwrap();
    // let kl_divergence = ndarray.kl_divergence(&baseline_ndarray).unwrap();
    // println!("KL-Divergence {}", kl_divergence);
}

fn main() {
    let docker = clients::Cli::default();

    let (database_connection_url, _postgres_node) = start_dockerized_postgres(&docker);

    seed_database(&database_connection_url);

    let baseline_result = query_data_into_dataframe(&database_connection_url, QUERY);
    println!("{}", baseline_result.head(Some(12)));

    println!("");
    println!("pgcloak baseline");
    benchmark(
        &docker,
        &database_connection_url,
        &PgcloakConfig {
            k: 3,
            columns: vec![],
            max_pool_size: 10,
        },
        &[],
        &baseline_result,
    );

    let column_configs = vec![
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
            name: String::from("adults.marital-status"),
        },
        ColumnConfiguration::PseudoIdentifier {
            name: String::from("adults.education"),
        },
    ];

    for i in 0..3 {
        let end = column_configs.len() - (2 - i);
        let columns = column_configs[0..end].to_vec();

        for k in vec![3, 10, 30, 50, 100, 250] {
            let column_names: Vec<String> = columns
                .iter()
                .filter_map(|c| match c {
                    ColumnConfiguration::PseudoIdentifier { name } => {
                        Some(name.replace("adults.", ""))
                    }
                    _ => None,
                })
                .collect();

            println!("");
            println!(
                "pgcloak k-anonymization | k={} | QI columns: {}",
                k,
                column_names.join(", ")
            );
            benchmark(
                &docker,
                &database_connection_url,
                &PgcloakConfig {
                    columns: columns.clone(),
                    max_pool_size: 10,
                    k,
                },
                &column_names,
                &baseline_result,
            );
        }
    }
}
