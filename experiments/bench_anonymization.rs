mod utils;

use crate::utils::benchmark::print_benchmark_stats;
use crate::utils::data::query_data_into_dataframe;
use crate::utils::docker::pgcloak::start_pgcloak;
use crate::utils::docker::pgcloak::ColumnConfiguration;
use crate::utils::docker::pgcloak::PgcloakConfig;
use crate::utils::docker::postgres::start_dockerized_postgres;
use itertools::iproduct;
use polars::prelude::DataFrame;
use postgres::{Client, NoTls};
use std::fs;
use std::time::Instant;
use testcontainers::clients::{self};

const QUERY: &str = "SELECT * FROM adults";
const ITERATIONS: i32 = 1;

fn seed_database(database_connection_url: &str) {
    let mut client = Client::connect(database_connection_url, NoTls).unwrap();
    let adults_data = "./experiments/resources/adults.sql";
    let contents = fs::read_to_string(adults_data).expect("Something went wrong reading the file");
    client.batch_execute(&contents).unwrap();
}

fn benchmark(
    docker: &clients::Cli,
    config: &PgcloakConfig,
) -> (DataFrame, Vec<(Instant, Instant)>) {
    let (database_connection_url, _postgres_node) = start_dockerized_postgres(&docker);

    seed_database(&database_connection_url);

    let (pgcloak_connection_url, _pgcloak_node, _pgcloak_tempdir) =
        start_pgcloak(docker, &database_connection_url, config);

    std::thread::sleep(std::time::Duration::from_millis(1000));

    let result = query_data_into_dataframe(&pgcloak_connection_url, QUERY);

    let durations = utils::benchmark::benchmark_function(ITERATIONS, &|| {
        let mut client = Client::connect(&pgcloak_connection_url, NoTls).unwrap();
        client.query(QUERY, &[]).unwrap();
    });
    print_benchmark_stats(&durations);

    drop(_pgcloak_node);
    drop(_pgcloak_tempdir);
    drop(_postgres_node);

    (result, durations)
}

fn main() {
    let docker = clients::Cli::default();

    let mut result_labels = vec![];
    let mut result_data = vec![];
    let mut result_ks = vec![];
    let mut result_columns = vec![];
    let mut result_durations = vec![];

    println!("");
    println!("baseline");
    let (data, durations) = benchmark(
        &docker,
        &PgcloakConfig {
            k: 3,
            columns: vec![],
            max_pool_size: 10,
        },
    );

    println!("{}", data.head(Some(12)));
    println!("");

    result_labels.push(String::from("baseline"));
    result_durations.push(durations);
    result_data.push(data);
    result_columns.push(vec![]);
    result_ks.push(-1);

    // let ks = vec![3, 10, 30, 50, 100, 250];
    let ks = vec![3, 30, 100];
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

    let columns: Vec<Vec<ColumnConfiguration>> = (0..2)
        .map(|i| {
            let end = column_configs.len() - (2 - i);
            let columns = column_configs[0..end].to_vec();
            columns
        })
        .collect();

    for (k, columns) in iproduct!(ks, columns) {
        let column_names: Vec<String> = columns
            .iter()
            .filter_map(|c| match c {
                ColumnConfiguration::PseudoIdentifier { name } => Some(name.replace("adults.", "")),
                _ => None,
            })
            .collect();

        println!("");
        println!("k={} | QI columns: {}", k, column_names.join(", "));
        let (data, durations) = benchmark(
            &docker,
            &PgcloakConfig {
                columns: columns.clone(),
                max_pool_size: 10,
                k,
            },
        );

        result_labels.push(format!("k={} | QI columns: {}", k, column_names.join(", ")));
        result_data.push(data);
        result_durations.push(durations);
        result_columns.push(column_names);
        result_ks.push(k as isize);
    }

    #[cfg(feature = "analysis")]
    {
        use inline_python::python;
        use itertools::Itertools;
        use polars::prelude::Utf8Type;
        use std::collections::HashMap;

        fn count_equivalence_classes(df: &DataFrame, columns: &[&str]) -> usize {
            let df = df.select(columns.to_vec()).unwrap();
            let df = df.drop_duplicates(true, None).unwrap();
            df.height()
        }

        fn get_equivalence_class_sizes(df: &DataFrame) -> Vec<i32> {
            let mut merged = vec![];
            for series in df.get_columns() {
                for (idx, v) in series
                    .cast::<Utf8Type>()
                    .unwrap()
                    .utf8()
                    .unwrap()
                    .into_iter()
                    .enumerate()
                {
                    if idx >= merged.len() {
                        merged.push(String::from(""))
                    }

                    merged[idx] = format!("{};{:?}", merged[idx], v)
                }
            }

            let mut map = HashMap::new();
            for value in merged {
                let counter = map.entry(value).or_insert(0);
                *counter += 1;
            }

            return map.values().cloned().collect();
        }

        fn calculate_discernibility_metric(df: &DataFrame, columns: &[&str]) -> usize {
            let df = df.select(columns.to_vec()).unwrap();
            let eq_class_sizes = get_equivalence_class_sizes(&df);

            assert_eq!(
                eq_class_sizes.len(),
                count_equivalence_classes(&df, &df.get_column_names())
            );

            eq_class_sizes
                .iter()
                .fold(0, |agg, value| agg + (value * value) as usize)
        }

        let classification_accuracies: Vec<f64> = result_data
            .iter()
            .zip(&result_columns)
            .map(|(df, qi_columns)| {
                use naivebayes::NaiveBayes;
                let mut nb = NaiveBayes::new();

                let classification_columns = vec![
                    "age",
                    "sex",
                    "education",
                    "occupation",
                    "hoursperweek",
                    "class",
                ];
                let df = df.select(&classification_columns).unwrap();

                let serieses: Vec<Vec<String>> = df
                    .get_columns()
                    .iter()
                    .map(|series| {
                        series
                            .cast::<Utf8Type>()
                            .unwrap()
                            .utf8()
                            .unwrap()
                            .into_iter()
                            .map(|v| v.map_or("None".to_string(), |s| s.to_string()))
                            .collect()
                    })
                    .collect();

                let data: (Vec<(Vec<String>, String)>) = (0..df.height())
                    .map(|idx| {
                        let tokens = serieses[..serieses.len() - 1]
                            .iter()
                            .map(|s| s[idx].clone())
                            .collect();
                        let label = serieses[serieses.len() - 1][idx].clone();
                        (tokens, label)
                    })
                    .collect();

                for (tokens, label) in data[500..].iter() {
                    nb.train(&tokens, &label);
                }

                let correctly_classified: Vec<bool> = data[..500]
                    .iter()
                    .map(|(tokens, label)| {
                        let classification = nb.classify(&tokens);
                        let opposite_label = if label == "<=50K" { ">50K" } else { "<=50K" };
                        let was_correct = classification[label] > classification[opposite_label];
                        was_correct
                    })
                    .collect();

                let accuracy = correctly_classified
                    .iter()
                    .filter(|v| **v)
                    .collect::<Vec<&bool>>()
                    .len() as f64
                    / correctly_classified.len() as f64;

                accuracy
            })
            .collect();

        let mut eq_class_counts = vec![];
        let mut average_eq_class_sizes = vec![];
        // let mut normalized_average_eq_class_sizes = vec![];
        let mut discernibility_metrics = vec![];

        for index in 0..result_data.len() {
            let columns = result_columns.get(index).unwrap();
            let data = result_data.get(index).unwrap();
            let k = result_ks.get(index).unwrap();

            if columns.len() > 0 {
                let qi_columns_strs = columns.iter().map(|c| c.as_str()).collect::<Vec<&str>>();

                let equivalence_class_count = count_equivalence_classes(&data, &qi_columns_strs);
                eq_class_counts.push(Some(equivalence_class_count));

                let average_eq_size = data.height() / equivalence_class_count;
                average_eq_class_sizes.push(Some(average_eq_size));

                // let normalized_average_eq_size = average_eq_size / *k as usize;
                // normalized_average_eq_class_sizes.push(Some(normalized_average_eq_size));

                let dm = calculate_discernibility_metric(&data, &qi_columns_strs);
                discernibility_metrics.push(Some(dm));

                // TODO: KL-Divergence
            } else {
                eq_class_counts.push(None);
                average_eq_class_sizes.push(None);
                // normalized_average_eq_class_sizes.push(None);
                discernibility_metrics.push(None);
            }
        }

        let mut groups: HashMap<
            String,
            (
                Vec<isize>,
                Vec<f64>,
                Vec<Option<usize>>,
                Vec<Option<usize>>,
                Vec<Option<usize>>,
                Vec<u128>,
                Vec<u128>,
            ),
        > = HashMap::new();
        for idx in (0..result_labels.len()) {
            let columns = &result_columns[idx];

            let key = if columns.len() < 1 {
                String::from("No anonymization")
            } else {
                format!("QI {}", columns.join(", "))
            };

            let durations = result_durations[idx].clone();
            let k = result_ks[idx];
            let accuracy = classification_accuracies[idx];
            let eq_class_count = eq_class_counts[idx];
            let average_eq_size = average_eq_class_sizes[idx];
            let discernibility_metric = discernibility_metrics[idx];

            let durations_in_ms: Vec<u128> = durations
                .iter()
                .map(|(end, start)| end.duration_since(*start).as_millis())
                .collect();

            let min_duration = *itertools::min(&durations_in_ms).unwrap();
            let mean_duration =
                durations_in_ms.iter().sum1::<u128>().unwrap() / durations_in_ms.len() as u128;

            let (
                grouped_ks,
                grouped_accuracies,
                grouped_eq_class_counts,
                grouped_average_eq_class_sizes,
                grouped_discernibility_metrics,
                grouped_min_durations,
                grouped_mean_durations,
            ) = groups.entry(key).or_default();

            grouped_accuracies.push(accuracy);
            grouped_ks.push(k);
            grouped_eq_class_counts.push(eq_class_count);
            grouped_average_eq_class_sizes.push(average_eq_size);
            grouped_discernibility_metrics.push(discernibility_metric);
            grouped_min_durations.push(min_duration);
            grouped_mean_durations.push(mean_duration);
        }

        let group_values: Vec<(
            Vec<isize>,
            Vec<f64>,
            Vec<Option<usize>>,
            Vec<Option<usize>>,
            Vec<Option<usize>>,
            Vec<u128>,
            Vec<u128>,
        )> = groups.values().cloned().collect();
        let group_labels: Vec<String> = groups.keys().cloned().collect();

        python! {
            import matplotlib.pyplot as plt
            import numpy as np
            import seaborn as sns
            from matplotlib.ticker import FormatStrFormatter
            from matplotlib.ticker import FuncFormatter

            def format_ms(x, pos):
                if x > 1800:
                    return "{0:.2f} s".format(x / 1000)
                return "{0} ms".format(x)

            fig = plt.figure()
            gs = fig.add_gridspec(ncols=6, nrows=2)

            ax = fig.add_subplot(gs[0, :3])
            ax.set_title("Query durations (%s iterations)" % 'ITERATIONS)

            for i, (ks, accuracies, eq_class_counts, average_eq_sizes, discernibility_metrics, min_durations, mean_durations) in enumerate('group_values):
                color = next(ax._get_lines.prop_cycler)["color"]
                label = 'group_labels[i]
                ax.plot(ks, min_durations, marker = "o", linestyle="-", color=color, label="%s (min)" % label)
                ax.plot(ks, mean_durations, marker = "x", linestyle="--", color=color, label="%s (mean)" % label)

            ax.legend()
            ax.get_yaxis().set_major_formatter(FuncFormatter(format_ms))
            ax.set_axisbelow(True)
            ax.get_yaxis().grid(True, color="#EEEEEE")
            ax.set_xlabel("k (-1 represents no anonymization)")

            ax = fig.add_subplot(gs[0, 3:])
            ax.set_title("Classification accuracy")
            for ks, accuracies, eq_class_counts, average_eq_sizes, discernibility_metrics, min_durations, mean_durations in 'group_values:
                ax.plot(ks, accuracies, marker = "o")
            ax.legend('group_labels)
            ax.set_axisbelow(True)
            ax.get_yaxis().grid(True, color="#EEEEEE")
            ax.set_xlabel("k (-1 represents no anonymization)")

            ax = fig.add_subplot(gs[1, :2])
            ax.set_title("Equivalence class count")
            for ks, accuracies, eq_class_counts, average_eq_sizes, discernibility_metrics, min_durations, mean_durations in 'group_values:
                ax.plot(ks, eq_class_counts, marker = "o")
            ax.legend('group_labels)
            ax.set_axisbelow(True)
            ax.get_yaxis().grid(True, color="#EEEEEE")
            ax.set_xlabel("k")

            ax = fig.add_subplot(gs[1, 2:4])
            ax.set_title("Average equivalence class sizes")
            for ks, accuracies, eq_class_counts, average_eq_sizes, discernibility_metrics, min_durations, mean_durations in 'group_values:
                ax.plot(ks, average_eq_sizes, marker = "o")
            ax.legend('group_labels)
            ax.set_axisbelow(True)
            ax.get_yaxis().grid(True, color="#EEEEEE")
            ax.set_xlabel("k")

            ax = fig.add_subplot(gs[1, 4:])
            ax.set_title("Discernibility Metric")
            for ks, accuracies, eq_class_counts, average_eq_sizes, discernibility_metrics, min_durations, mean_durations in 'group_values:
                ax.plot(ks, discernibility_metrics, marker = "o")
            ax.legend('group_labels)
            ax.set_axisbelow(True)
            ax.get_yaxis().grid(True, color="#EEEEEE")
            ax.set_xlabel("k")

            plt.tight_layout()
            plt.show()
        }
    }
}
