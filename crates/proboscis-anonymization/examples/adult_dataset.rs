use polars::prelude::CsvReader;
use polars::prelude::SerReader;
use proboscis_anonymization::anonymize;
use proboscis_anonymization::AnonymizationCriteria;
use proboscis_anonymization::NumericAggregation;

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
        &NumericAggregation::Range,
    );

    println!("{:?}", anonymized.head(Some(25)));

    Ok(())
}
