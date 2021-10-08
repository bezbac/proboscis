use anonymization::anonymize;
use anonymization::AnonymizationCriteria;
use polars::prelude::CsvReader;
use polars::prelude::SerReader;

fn main() -> anyhow::Result<()> {
    let csv_path = "examples/adult.csv".to_string();

    let df = CsvReader::from_path(csv_path)?
        .infer_schema(None)
        .has_header(true)
        .finish()?;

    println!("{:?}", df.head(None));

    let quasi_identifiers = vec!["age", "workclass", "education", "native-country"];
    let anonymized = anonymize(
        &df,
        &quasi_identifiers,
        &[AnonymizationCriteria::KAnonymous { k: 3 }],
    );

    println!("{:?}", anonymized.head(None));

    Ok(())
}
