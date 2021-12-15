mod utils;

use crate::utils::data::query_data_into_dataframe;
use crate::utils::docker::pgcloak::start_pgcloak;
use crate::utils::docker::pgcloak::ColumnConfiguration;
use crate::utils::docker::pgcloak::PgcloakConfig;
use crate::utils::docker::postgres::start_dockerized_postgres;
use polars::prelude::DataFrame;
use postgres::{Client, NoTls};
use std::fs;
use std::time::Duration;
use std::time::Instant;
use testcontainers::clients::{self};

const QUERY: &str = "SELECT * FROM adults";

fn seed_database(database_connection_url: &str) {
    let mut client = Client::connect(database_connection_url, NoTls).unwrap();
    let adults_data = "./experiments/resources/adults.sql";
    let contents = fs::read_to_string(adults_data).expect("Something went wrong reading the file");
    client.batch_execute(&contents).unwrap();
}

fn benchmark(
    docker: &clients::Cli,
    database_connection_url: &str,
    config: &PgcloakConfig,
) -> (DataFrame, Duration) {
    let (pgcloak_connection_url, _pgcloak_node, _pgcloak_tempdir) =
        start_pgcloak(docker, database_connection_url, config);

    let before = Instant::now();
    let result = query_data_into_dataframe(&pgcloak_connection_url, QUERY);
    let after = Instant::now();

    // let (docker_stats, baseline_durations) =
    //     utils::docker::stats::while_collecting_docker_stats(_pgcloak_node.id(), &|| {
    //         utils::benchmark::benchmark_function(10, &|| {
    //             let mut client = Client::connect(&pgcloak_connection_url, NoTls).unwrap();
    //             let result = client.query(QUERY, &[]);
    //             if result.is_err() {
    //                 println!("Error: {:?}", result.unwrap_err())
    //             }
    //             drop(client)
    //         })
    //     });
    // print_benchmark_stats(&baseline_durations);

    drop(_pgcloak_node);
    drop(_pgcloak_tempdir);

    (result, after.duration_since(before))
}

fn main() {
    let docker = clients::Cli::default();

    let (database_connection_url, _postgres_node) = start_dockerized_postgres(&docker);

    seed_database(&database_connection_url);

    let mut result_labels = vec![];
    let mut result_data = vec![];
    let mut result_ks = vec![];
    let mut result_columns = vec![];
    let mut result_durations = vec![];

    println!("");
    println!("pgcloak baseline");
    let (data, duration) = benchmark(
        &docker,
        &database_connection_url,
        &PgcloakConfig {
            k: 3,
            columns: vec![],
            max_pool_size: 10,
        },
    );

    println!("{}", data.head(Some(12)));
    println!("");

    result_labels.push(String::from("baseline"));
    result_durations.push(duration);
    result_data.push(data);
    result_columns.push(vec![]);
    result_ks.push(None);

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

    // TODO: Different column configs
    // for i in 0..3 {
    //     let end = column_configs.len() - (2 - i);
    //     let columns = column_configs[0..end].to_vec();
    // }

    let columns = column_configs;

    let ks = vec![3, 10, 30, 50, 100, 250];
    for k in ks {
        let column_names: Vec<String> = columns
            .iter()
            .filter_map(|c| match c {
                ColumnConfiguration::PseudoIdentifier { name } => Some(name.replace("adults.", "")),
                _ => None,
            })
            .collect();

        println!("");
        println!(
            "pgcloak k-anonymization | k={} | QI columns: {}",
            k,
            column_names.join(", ")
        );
        let (data, duration) = benchmark(
            &docker,
            &database_connection_url,
            &PgcloakConfig {
                columns: columns.clone(),
                max_pool_size: 10,
                k,
            },
        );

        result_labels.push(format!(
            "k-anonymization | k={} | QI columns: {}",
            k,
            column_names.join(", ")
        ));
        result_data.push(data);
        result_durations.push(duration);
        result_columns.push(column_names);
        result_ks.push(Some(k));
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

        let durations_in_milis: Vec<u128> = result_durations
            .iter()
            .map(|duration| duration.as_millis())
            .collect();

        let classification_accuracies: Vec<f64> = result_data
            .iter()
            .map(|df| {
                use naivebayes::NaiveBayes;
                let mut nb = NaiveBayes::new();

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
        let mut normalized_average_eq_class_sizes = vec![];
        let mut discernibility_metrics = vec![];

        for index in 0..result_data.len() {
            let columns = result_columns.get(index).unwrap();
            let data = result_data.get(index).unwrap();

            if columns.len() > 0 {
                let k = result_ks.get(index).unwrap().unwrap();

                let qi_columns_strs = columns.iter().map(|c| c.as_str()).collect::<Vec<&str>>();

                let equivalence_class_count = count_equivalence_classes(&data, &qi_columns_strs);
                eq_class_counts.push(Some(equivalence_class_count));

                let average_eq_size = data.height() / equivalence_class_count;
                average_eq_class_sizes.push(Some(average_eq_size));

                let normalized_average_eq_size = average_eq_size / k;
                normalized_average_eq_class_sizes.push(Some(normalized_average_eq_size));

                let dm = calculate_discernibility_metric(&data, &qi_columns_strs);
                discernibility_metrics.push(Some(dm));

                // TODO: KL-Divergence
            } else {
                eq_class_counts.push(None);
                average_eq_class_sizes.push(None);
                normalized_average_eq_class_sizes.push(None);
                discernibility_metrics.push(None);
            }
        }

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
            gs = fig.add_gridspec(ncols=2, nrows=3)

            ax = fig.add_subplot(gs[0, :])
            ax.set_title("Query durations")
            ax.plot('result_labels, 'durations_in_milis, marker = "o")
            ax.get_yaxis().set_major_formatter(FuncFormatter(format_ms))
            ax.set_axisbelow(True)
            ax.get_yaxis().grid(True, color="#EEEEEE")

            ax = fig.add_subplot(gs[1, 0])
            ax.set_title("Equivalence class count")
            ax.plot('result_labels, 'eq_class_counts, marker = "o")
            ax.set_axisbelow(True)
            ax.get_yaxis().grid(True, color="#EEEEEE")

            ax = fig.add_subplot(gs[1, 1])
            ax.set_title("Average equivalence class sizes")
            ax.plot('result_labels, 'average_eq_class_sizes, marker = "o")
            ax.set_axisbelow(True)
            ax.get_yaxis().grid(True, color="#EEEEEE")

            ax = fig.add_subplot(gs[2, 0])
            ax.set_title("Discernibility Metric")
            ax.plot('result_labels, 'discernibility_metrics, marker = "o")
            ax.set_axisbelow(True)
            ax.get_yaxis().grid(True, color="#EEEEEE")

            ax = fig.add_subplot(gs[2, 1])
            ax.set_title("Classification accuracy")
            ax.plot('result_labels, 'classification_accuracies, marker = "o")
            ax.set_axisbelow(True)
            ax.get_yaxis().grid(True, color="#EEEEEE")

            plt.tight_layout()
            plt.show()
        }
    }
}
