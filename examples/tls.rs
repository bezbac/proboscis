use anyhow::Result;
use maplit::hashmap;
use proboscis::{
    postgres_resolver::{PostgresResolver, TargetConfig},
    Config, Proxy,
};
use tokio_postgres::{NoTls, SimpleQueryMessage};

async fn migrations() {
    mod embedded {
        use refinery::embed_migrations;
        embed_migrations!("examples/sql_migrations");
    }

    let (mut client, connection) =
        tokio_postgres::connect("host=0.0.0.0 port=5432 user=admin password=password", NoTls)
            .await
            .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    embedded::migrations::runner()
        .run_async(&mut client)
        .await
        .expect("Migrations failed");
}

async fn proxy() -> Result<()> {
    let postgres_resolver = PostgresResolver::new(
        TargetConfig::from_uri("postgres://admin:password@0.0.0.0:5432/postgres")?,
        20,
    )
    .await?;

    let mut proxy = Proxy::new(
        Config {
            credentials: hashmap! {
                "admin".to_string() => "password".to_string(),
            },
            tls_config: Some(proboscis::TlsConfig {
                pcks_path: "examples/openssl/identity.p12".to_string(),
                password: "password".to_string(),
            }),
        },
        Box::new(postgres_resolver),
    );

    proxy.listen("0.0.0.0:5430").await
}

#[tokio::main]
async fn main() {
    tokio::join!(migrations());
    tokio::spawn(proxy());

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let connector = postgres_native_tls::MakeTlsConnector::new(connector);

    let (client, connection) = tokio_postgres::connect(
        "host=0.0.0.0 port=5430 user=admin password=password sslmode=require",
        connector,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Simple query
    let simple_query_result = client
        .simple_query("SELECT id, name FROM users")
        .await
        .unwrap();

    let row = match simple_query_result.first().unwrap() {
        SimpleQueryMessage::Row(v) => v,
        _ => panic!("Not a row"),
    };

    let name: &str = row.get(1).unwrap();
    assert_eq!(name, "Max");

    // Normal query
    let rows = client
        .query("SELECT id, name FROM users", &[])
        .await
        .unwrap();

    let name: &str = rows[0].get(1);
    assert_eq!(name, "Max");
}
