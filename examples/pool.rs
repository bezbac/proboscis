use maplit::hashmap;
use proboscis::{
    postgres_resolver::{PostgresResolver, TargetConfig},
    Config, Proxy,
};
use testcontainers::clients;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

mod setup;

async fn run_proxy(database_connection_url: String) -> String {
    let proxy_user = "admin";
    let proxy_password = "password";

    let mut proxy = Proxy::new(
        Config {
            credentials: hashmap! {
                proxy_user.to_string() => proxy_password.to_string(),
            },
            tls_config: None,
        },
        Box::new(
            PostgresResolver::new(
                TargetConfig::from_uri(&database_connection_url).unwrap(),
                20,
            )
            .await
            .unwrap(),
        ),
    );

    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();

    let proxy_connection_url = format!(
        "postgres://{}:{}@localhost:{}",
        proxy_user,
        proxy_password,
        listener.local_addr().unwrap().port()
    );

    tokio::spawn(async move {
        if let Err(e) = proxy.listen(listener).await {
            eprintln!("proxy error: {}", e);
        }
    });

    proxy_connection_url
}

#[tokio::main]
async fn main() {
    let docker = clients::Cli::default();

    let (database_connection_url, _node) = setup::start_dockerized_postgres(&docker).await;
    let proxy_connection_url = run_proxy(database_connection_url).await;

    let (client, connection) = tokio_postgres::connect(&proxy_connection_url, NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    for _ in 0..1000 {
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
    }
}
