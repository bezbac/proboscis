mod utils;

use crate::utils::{
    benchmark::print_benchmark_stats,
    docker::{start_dockerized_postgres, start_pgbouncer, start_pgcloak, start_pgpool},
};
use postgres::NoTls;
use std::time::Instant;
use testcontainers::clients::{self};

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
    let baseline_durations =
        utils::benchmark::benchmark_function(&|| simple_query(&database_connection_url));
    print_benchmark_stats(&baseline_durations);

    // postgres 13.4 (pgcloak - session pooling - 10 max connections)
    println!("pgcloak");
    let (pgcloak_connection_url, _pgcloak_node, _pgcloak_tempdir) =
        start_pgcloak(&docker, &database_connection_url);
    let (docker_stats, pgcloak_durations) =
        utils::docker::while_collecting_docker_stats(_pgcloak_node.id(), &|| {
            utils::benchmark::benchmark_function(&|| simple_query(&pgcloak_connection_url))
        });
    print_benchmark_stats(&pgcloak_durations);
    drop(_pgcloak_node);
    drop(_pgcloak_tempdir);

    // postgres 13.4 (pg_pool)
    println!("pgpool");
    let (pgpool_connection_url, _pgpool_node) = start_pgpool(&docker, &database_connection_url);
    let (docker_stats, pgpool_durations) =
        utils::docker::while_collecting_docker_stats(_pgpool_node.id(), &|| {
            utils::benchmark::benchmark_function(&|| simple_query(&pgpool_connection_url))
        });
    print_benchmark_stats(&pgpool_durations);
    drop(_pgpool_node);

    // postgres 13.4 (pg_bouncer - session pooling - 10 max connections)
    println!("pgbouncer");
    let (pgbouncer_connection_url, _pgbouncer_node) =
        start_pgbouncer(&docker, &database_connection_url);
    let (docker_stats, pgbouncer_durations) =
        utils::docker::while_collecting_docker_stats(_pgbouncer_node.id(), &|| {
            utils::benchmark::benchmark_function(&|| simple_query(&pgbouncer_connection_url))
        });
    print_benchmark_stats(&pgbouncer_durations);
    drop(_pgbouncer_node);

    if cfg!(feature = "analysis") {
        use inline_python::python;

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

        python! {
            import matplotlib.pyplot as plt

            plt.subplot(2, 1, 1)
            plt.title("Comparison")
            plt.plot('total_times)
            plt.ylabel("ms")

            plt.show()
        }
    }
}
