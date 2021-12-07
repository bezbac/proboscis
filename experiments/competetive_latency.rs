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
    let iterations = 1000;

    let docker = clients::Cli::default();

    let (database_connection_url, _postgres_node) = start_dockerized_postgres(&docker);

    // postgres 13.4 (no proxy)
    println!("no proxy");
    let baseline_durations = utils::benchmark::benchmark_function(iterations, &|| {
        simple_query(&database_connection_url)
    });
    print_benchmark_stats(&baseline_durations);

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
    let (docker_stats, pgcloak_durations) =
        utils::docker::stats::while_collecting_docker_stats(_pgcloak_node.id(), &|| {
            utils::benchmark::benchmark_function(iterations, &|| {
                simple_query(&pgcloak_connection_url)
            })
        });
    print_benchmark_stats(&pgcloak_durations);
    drop(_pgcloak_node);
    drop(_pgcloak_tempdir);

    // postgres 13.4 (pg_pool)
    println!("pgpool");
    let (pgpool_connection_url, _pgpool_node) = start_pgpool(&docker, &database_connection_url);
    let (docker_stats, pgpool_durations) =
        utils::docker::stats::while_collecting_docker_stats(_pgpool_node.id(), &|| {
            utils::benchmark::benchmark_function(iterations, &|| {
                simple_query(&pgpool_connection_url)
            })
        });
    print_benchmark_stats(&pgpool_durations);
    drop(_pgpool_node);

    // postgres 13.4 (pg_bouncer - session pooling - 10 max connections)
    println!("pgbouncer");
    let (pgbouncer_connection_url, _pgbouncer_node) =
        start_pgbouncer(&docker, &database_connection_url);
    let (docker_stats, pgbouncer_durations) =
        utils::docker::stats::while_collecting_docker_stats(_pgbouncer_node.id(), &|| {
            utils::benchmark::benchmark_function(iterations, &|| {
                simple_query(&pgbouncer_connection_url)
            })
        });
    print_benchmark_stats(&pgbouncer_durations);
    drop(_pgbouncer_node);

    #[cfg(feature = "analysis")]
    {
        use inline_python::python;
        use itertools::Itertools;
        use std::time::Instant;

        fn calculate_total_time(durations: &[(Instant, Instant)]) -> u128 {
            let first_time = durations.first().unwrap().1;
            let last_time = durations.last().unwrap().0;
            return last_time.duration_since(first_time).as_millis();
        }

        let mut total_times = vec![];
        total_times.push(calculate_total_time(&baseline_durations));
        total_times.push(calculate_total_time(&pgcloak_durations));
        total_times.push(calculate_total_time(&pgpool_durations));
        total_times.push(calculate_total_time(&pgbouncer_durations));

        // fn calculate_stats(durations: &[(Instant, Instant)]) -> (u128, u128, u128) {
        //     let durations_in_milis: Vec<u128> = durations
        //         .iter()
        //         .map(|(end, start)| end.duration_since(*start).as_millis())
        //         .collect();

        //     let mean = durations_in_milis.iter().sum1::<u128>().unwrap()
        //         / durations_in_milis.len() as u128;
        //     let min = *durations_in_milis.iter().min().unwrap();
        //     let max = *durations_in_milis.iter().max().unwrap();

        //     (mean, min, max)
        // }

        // let mut stats = vec![];
        // stats.push(calculate_stats(&baseline_durations));
        // stats.push(calculate_stats(&pgcloak_durations));
        // stats.push(calculate_stats(&pgpool_durations));
        // stats.push(calculate_stats(&pgbouncer_durations));

        fn durations_in_milis(durations: &[(Instant, Instant)]) -> Vec<u128> {
            durations
                .iter()
                .map(|(end, start)| end.duration_since(*start).as_millis())
                .collect()
        }

        let baseline_durations_in_ms = durations_in_milis(&baseline_durations);
        let pgcloak_durations_in_ms = durations_in_milis(&pgcloak_durations);
        let pgpool_durations_in_ms = durations_in_milis(&pgpool_durations);
        let pgbouncer_durations_in_ms = durations_in_milis(&pgbouncer_durations);

        python! {
            import matplotlib.pyplot as plt
            import numpy as np
            import seaborn as sns

            from matplotlib.ticker import FormatStrFormatter

            # Total Duration
            fig, ax = plt.subplots()
            plt.title("Total benchmark duration (%s iterations)" % 'iterations)
            labels = ["no-proxy", "pgcloak", "pgpool", "pgbouncer"]

            ax.bar(labels, 'total_times)
            ax.bar_label(ax.containers[0])
            ax.get_yaxis().set_major_formatter(FormatStrFormatter("%d ms"))

            # Violin Plot
            fig, ax = plt.subplots()
            ax = sns.violinplot(data=['baseline_durations_in_ms, 'pgcloak_durations_in_ms, 'pgpool_durations_in_ms, 'pgbouncer_durations_in_ms])

            plt.show()
        }
    }
}
