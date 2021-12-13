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
use std::time::Instant;
use testcontainers::clients::{self};

fn simple_query(connection_url: &str) {
    let mut client = postgres::Client::connect(connection_url, NoTls).unwrap();

    let rows = client.query("SELECT 0, 'Alex'", &[]).unwrap();

    let sum: &str = rows[0].get(1);
    assert_eq!(sum, "Alex");
}

fn main() {
    let experiment_start_time = Instant::now();

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

    // // postgres 13.4 (pgcloak - session pooling - 10 max connections)
    // println!("pgcloak");
    // let (pgcloak_connection_url, _pgcloak_node, _pgcloak_tempdir) = start_pgcloak(
    //     &docker,
    //     &database_connection_url,
    //     &PgcloakConfig {
    //         k: 3,
    //         columns: vec![],
    //         max_pool_size: 10,
    //     },
    // );
    // let (pgcloak_docker_stats, pgcloak_durations) =
    //     utils::docker::stats::while_collecting_docker_stats(_pgcloak_node.id(), &|| {
    //         utils::benchmark::benchmark_function(iterations, &|| {
    //             simple_query(&pgcloak_connection_url)
    //         })
    //     });
    // print_benchmark_stats(&pgcloak_durations);
    // result_labels.push("pgcloak");
    // result_durations.push(pgcloak_durations);
    // result_docker_stats.push(Some(pgcloak_docker_stats));
    // drop(_pgcloak_node);
    // drop(_pgcloak_tempdir);

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
        use std::time::UNIX_EPOCH;

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

        let memory_stats: Vec<(&str, Vec<u128>, Vec<u64>)> = result_labels
            .iter()
            .zip(&result_docker_stats)
            .filter_map(|(label, data)| {
                data.as_ref().map(|data| {
                    (
                        *label,
                        data.iter()
                            .map(|(time, _)| time.duration_since(experiment_start_time).as_millis())
                            .collect(),
                        data.iter()
                            .map(|(_, stats)| {
                                stats.memory_stats.usage - stats.memory_stats.stats.cache
                            })
                            .collect(),
                    )
                })
            })
            .collect();

        let cpu_stats: Vec<(&str, Vec<u128>, Vec<f64>)> = result_labels
            .iter()
            .zip(&result_docker_stats)
            .filter_map(|(label, data)| {
                data.as_ref().map(|data| {
                    (
                        *label,
                        data.iter()
                            .map(|(time, _)| time.duration_since(experiment_start_time).as_millis())
                            .collect(),
                        data.iter()
                            .enumerate()
                            .map(|(i, (_, stats))| {
                                let previous_stats = if (i > 1) {
                                    &data.get(i - 1).unwrap().1
                                } else {
                                    stats
                                };

                                let precpu_stats = &previous_stats.cpu_stats;
                                let cpu_stats = &stats.cpu_stats;

                                let cpu_delta = cpu_stats.cpu_usage.total_usage
                                    - precpu_stats.cpu_usage.total_usage;
                                let system_cpu_delta =
                                    cpu_stats.system_cpu_usage - precpu_stats.system_cpu_usage;
                                let number_cpus = cpu_stats.cpu_usage.percpu_usage.len();
                                let usage_percent = (cpu_delta as f64 / system_cpu_delta as f64)
                                    * number_cpus as f64
                                    * 100.0;
                                usage_percent
                            })
                            .collect(),
                    )
                })
            })
            .collect();

        let run_stats: Vec<(&str, Vec<u128>, Vec<u128>)> = result_labels
            .iter()
            .zip(&result_durations)
            .map(|(label, durations)| {
                (
                    *label,
                    durations
                        .iter()
                        .map(|(end_time, start_time)| {
                            end_time.duration_since(experiment_start_time).as_millis()
                        })
                        .collect(),
                    durations
                        .iter()
                        .map(|(end_time, start_time)| {
                            end_time.duration_since(*start_time).as_millis()
                        })
                        .collect(),
                )
            })
            .collect();

        python! {
            import matplotlib.pyplot as plt
            import numpy as np
            import seaborn as sns
            from matplotlib.ticker import FormatStrFormatter
            from matplotlib.ticker import FuncFormatter

            def format_bytes(b, pos):
                """Return the given bytes as a human friendly KB, MB, GB, or TB string."""
                b = float(b)
                KB = float(1024)
                MB = float(KB ** 2) # 1,048,576
                GB = float(KB ** 3) # 1,073,741,824
                TB = float(KB ** 4) # 1,099,511,627,776

                if b < KB:
                    return "{0} {1}".format(b,"Bytes" if 0 == b > 1 else "Byte")
                elif KB <= b < MB:
                    return "{0:.2f} KB".format(b / KB)
                elif MB <= b < GB:
                    return "{0:.2f} MB".format(b / MB)
                elif GB <= b < TB:
                    return "{0:.2f} GB".format(b / GB)
                elif TB <= b:
                    return "{0:.2f} TB".format(b / TB)

                return ""

            fig, axs = plt.subplots(1, 2)
            axs[0].set_title("Total benchmark duration (%s iterations)" % 'iterations)
            axs[0].bar('result_labels, 'total_times)
            axs[0].bar_label(axs[0].containers[0])
            axs[0].get_yaxis().set_major_formatter(FormatStrFormatter("%d ms"))

            axs[1].set_title("Individual run duration distribution (%s iterations)" % 'iterations)
            axs[1] = sns.violinplot(data='durations_in_milis)
            axs[1].set_xticklabels('result_labels)
            axs[1].get_yaxis().set_major_formatter(FormatStrFormatter("%d ms"))

            fig, axs = plt.subplots(3, len('run_stats))
            for ax in axs.flat:
                ax.set_axis_off()

            for index, chart in enumerate('run_stats):
                ax = axs[0, index]
                ax.set_axis_on()
                ax.set_title("Individual run duration (%s)" % chart[0])
                ax.scatter(chart[1], chart[2])
                ax.get_yaxis().set_major_formatter(FormatStrFormatter("%d ms"))
                ax.get_xaxis().set_major_formatter(FormatStrFormatter("%d ms"))
                ax.get_xaxis().set_major_locator(plt.MaxNLocator(2))

            for index, chart in enumerate('memory_stats):
                ax = axs[1, index + 1]
                ax.set_axis_on()
                ax.set_title("Memory usage during benchmark (%s)" % chart[0])
                ax.plot(chart[1], chart[2], label = "Used memory")
                ax.get_yaxis().set_major_formatter(FuncFormatter(format_bytes))
                ax.get_xaxis().set_major_formatter(FormatStrFormatter("%d ms"))
                ax.get_xaxis().set_major_locator(plt.MaxNLocator(2))

            for index, chart in enumerate('cpu_stats):
                ax = axs[2, index + 1]
                ax.set_axis_on()
                ax.set_title("CPU usage during benchmark (%s)" % chart[0])
                ax.plot(chart[1], chart[2], label = "CPU")
                ax.get_yaxis().set_major_formatter(FormatStrFormatter("%d %%"))
                ax.get_xaxis().set_major_formatter(FormatStrFormatter("%d ms"))
                ax.get_xaxis().set_major_locator(plt.MaxNLocator(2))


            plt.tight_layout()
            plt.show()
        }
    }
}
