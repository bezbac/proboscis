mod utils;

use crate::utils::docker::pgcloak::PgcloakConfig;
use crate::utils::{
    benchmark::print_benchmark_stats,
    docker::{
        pgbouncer::start_pgbouncer, pgcloak::start_pgcloak, pgpool::start_pgpool,
        postgres::start_dockerized_postgres,
    },
};
use postgres::NoTls;
use testcontainers::clients::{self};

fn simple_query(connection_url: &str) {
    let mut client = postgres::Client::connect(connection_url, NoTls).unwrap();

    let rows = client.query("SELECT 0, 'Alex'", &[]).unwrap();

    let sum: &str = rows[0].get(1);
    assert_eq!(sum, "Alex");
}

fn main() {
    let mut result_labels = vec![];
    let mut result_durations = vec![];
    let mut result_docker_stats = vec![];

    let iterations = 1000;

    let docker = clients::Cli::default();

    let (database_connection_url, _postgres_node) = start_dockerized_postgres(&docker);

    // postgres 13.4 (no proxy)
    println!("no proxy");
    let baseline_durations = utils::benchmark::benchmark_function(iterations, &|| {
        simple_query(&database_connection_url)
    });
    print_benchmark_stats(&baseline_durations);
    result_labels.push("no proxy");
    result_durations.push(baseline_durations);
    result_docker_stats.push(None);

    // postgres 13.4 (pgcloak - session pooling - 10 max connections)
    println!("pgcloak");
    let (pgcloak_connection_url, _pgcloak_node, _pgcloak_tempdir) = start_pgcloak(
        &docker,
        &database_connection_url,
        &PgcloakConfig {
            k: 3,
            columns: vec![],
            max_pool_size: 10,
        },
    );
    let (pgcloak_docker_stats, pgcloak_durations) =
        utils::docker::stats::while_collecting_docker_stats(_pgcloak_node.id(), &|| {
            utils::benchmark::benchmark_function(iterations, &|| {
                simple_query(&pgcloak_connection_url)
            })
        });
    print_benchmark_stats(&pgcloak_durations);
    result_labels.push("pgcloak");
    result_durations.push(pgcloak_durations);
    result_docker_stats.push(Some(pgcloak_docker_stats));
    drop(_pgcloak_node);
    drop(_pgcloak_tempdir);

    // postgres 13.4 (pg_pool)
    println!("pgpool");
    let (pgpool_connection_url, _pgpool_node) = start_pgpool(&docker, &database_connection_url);
    let (pgpool_docker_stats, pgpool_durations) =
        utils::docker::stats::while_collecting_docker_stats(_pgpool_node.id(), &|| {
            utils::benchmark::benchmark_function(iterations, &|| {
                simple_query(&pgpool_connection_url)
            })
        });
    print_benchmark_stats(&pgpool_durations);
    result_labels.push("pgpool");
    result_durations.push(pgpool_durations);
    result_docker_stats.push(Some(pgpool_docker_stats));
    drop(_pgpool_node);

    // postgres 13.4 (pg_bouncer - session pooling - 10 max connections)
    println!("pgbouncer");
    let (pgbouncer_connection_url, _pgbouncer_node) =
        start_pgbouncer(&docker, &database_connection_url);
    let (pgbouncer_docker_stats, pgbouncer_durations) =
        utils::docker::stats::while_collecting_docker_stats(_pgbouncer_node.id(), &|| {
            utils::benchmark::benchmark_function(iterations, &|| {
                simple_query(&pgbouncer_connection_url)
            })
        });
    print_benchmark_stats(&pgbouncer_durations);
    result_labels.push("pgbouncer");
    result_durations.push(pgbouncer_durations);
    result_docker_stats.push(Some(pgbouncer_docker_stats));
    drop(_pgbouncer_node);

    #[cfg(feature = "analysis")]
    {
        use inline_python::python;
        use itertools::Itertools;
        use std::time::Instant;

        let total_times: Vec<u128> = result_durations
            .iter()
            .map(|durations| {
                let first_time = durations.first().unwrap().1;
                let last_time = durations.last().unwrap().0;
                last_time.duration_since(first_time).as_millis()
            })
            .collect();

        let durations_in_milis: Vec<Vec<u128>> = result_durations
            .iter()
            .map(|durations| {
                durations
                    .iter()
                    .map(|(end, start)| end.duration_since(*start).as_millis())
                    .collect()
            })
            .collect();

        let memory_stats: Vec<(&str, Vec<u64>)> = result_labels
            .iter()
            .zip(&result_docker_stats)
            .filter_map(|(label, data)| {
                data.as_ref().map(|data| {
                    (
                        *label,
                        data.iter()
                            .map(|(_, stats)| stats.memory_stats.usage)
                            .collect(),
                    )
                })
            })
            .collect();

        let cpu_stats: Vec<(&str, Vec<u64>)> = result_labels
            .iter()
            .zip(&result_docker_stats)
            .filter_map(|(label, data)| {
                data.as_ref().map(|data| {
                    (
                        *label,
                        data.iter()
                            .map(|(_, stats)| stats.cpu_stats.cpu_usage.total_usage)
                            .collect(),
                    )
                })
            })
            .collect();

        python! {
            import matplotlib.pyplot as plt
            import numpy as np
            import seaborn as sns
            from matplotlib.ticker import FormatStrFormatter

            # Total Duration
            fig, ax = plt.subplots()
            plt.title("Total benchmark duration (%s iterations)" % 'iterations)

            ax.bar('result_labels, 'total_times)
            ax.bar_label(ax.containers[0])
            ax.get_yaxis().set_major_formatter(FormatStrFormatter("%d ms"))

            # Violin Plot
            fig, ax = plt.subplots()
            plt.title("Individual run duration (%s iterations)" % 'iterations)

            ax = sns.violinplot(data='durations_in_milis)
            ax.set_xticklabels('result_labels)
            ax.get_yaxis().set_major_formatter(FormatStrFormatter("%d ms"))

            # Stats
            for chart in 'memory_stats:
                fig, ax = plt.subplots()
                plt.title("Memory usage during benchmark (%s)" % chart[0])
                plt.plot(chart[1], label = "Used memory")

            for chart in 'cpu_stats:
                fig, ax = plt.subplots()
                plt.title("CPU usage during benchmark (%s)" % chart[0])
                plt.plot(chart[1], label = "CPU")

            plt.show()
        }
    }
}
