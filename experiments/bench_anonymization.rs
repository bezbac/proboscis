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

pub fn record_batch_to_data_frame(data: &RecordBatch) -> DataFrame {
    use polars::prelude::*;

    let series: Vec<Series> = data
        .columns()
        .iter()
        .zip(data.schema().fields())
        .map(|(column, field)| {
            Series::try_from((field.name().as_str(), vec![column.clone()])).unwrap()
        })
        .collect();

    DataFrame::new(series).unwrap()
}

fn query_data_into_dataframe(
    postgres_connection_string: &str,
    queries: &[&str],
) -> polars::frame::DataFrame {
    use connectorx::prelude::*;
    use connectorx::sources::postgres::{rewrite_tls_args, BinaryProtocol, PostgresSource};
    use url::Url;

    let url = Url::parse(postgres_connection_string).unwrap();
    let (config, _tls) = rewrite_tls_args(&url).unwrap();

    let mut destination = ArrowDestination::new();
    let source = PostgresSource::new(config, NoTls, 1).expect("cannot create the source");

    let dispatcher = Dispatcher::<
        PostgresSource<BinaryProtocol, NoTls>,
        ArrowDestination,
        PostgresArrowTransport<BinaryProtocol, NoTls>,
    >::new(source, &mut destination, queries, None);

    dispatcher.run().expect("run failed");

    let data = destination.arrow().unwrap();

    record_batch_to_data_frame(&data.first().unwrap())
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
