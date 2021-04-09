#[cfg(feature = "e2e")]
use postgres::{Client, Error, NoTls, SimpleQueryMessage, SimpleQueryRow};
use std::collections::HashMap;
use std::thread;

#[test]
#[cfg(feature = "e2e")]
fn test_general_use() -> Result<(), Error> {
    thread::spawn(|| {
        // Launch proxy
        let mut authentication = HashMap::new();
        authentication.insert("admin".to_string(), "password".to_string());

        let config = proboscis::Config {
            target_addr: "0.0.0.0:5432".to_string(),
            authentication,
        };

        let app = proboscis::App::new(config.clone());
        app.listen("0.0.0.0:5430");
    });

    let mut client = Client::connect("host=0.0.0.0 port=5430 user=admin password=password", NoTls)?;

    // Simple query
    let simple_query_result = client.simple_query("SELECT id, name FROM person")?;
    let row: &SimpleQueryRow = match simple_query_result.first().unwrap() {
        SimpleQueryMessage::Row(v) => v,
        _ => panic!("Not a row"),
    };

    let name: &str = row.get(1).unwrap();
    assert_eq!(name, "Max");

    // Normal query
    let query_result = client.query("SELECT id, name FROM person", &[])?;
    let row = query_result.first().unwrap();

    Ok(())
}
