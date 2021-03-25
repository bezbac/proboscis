#[cfg(feature = "e2e")]
use postgres::{Client, Error, NoTls, SimpleQueryMessage, SimpleQueryRow};

#[test]
#[cfg(feature = "e2e")]
fn test_end_to_end() -> Result<(), Error> {
    use std::collections::HashMap;
    use std::thread;

    thread::spawn(|| {
        // Launch proxy
        let mut authentication = HashMap::new();
        authentication.insert("admin".to_string(), "password".to_string());

        let config = proboscis::Config {
            target_addr: "0.0.0.0:5432".to_string(),
            authentication,
        };

        let app = proboscis::new(config.clone());
        app.listen("0.0.0.0:5430");
    });

    let mut client = Client::connect("host=0.0.0.0 port=5430 user=admin password=password", NoTls)?;

    let result = client.simple_query("SELECT id, name FROM person")?;

    let row: &SimpleQueryRow = match result.first().unwrap() {
        SimpleQueryMessage::Row(v) => v,
        _ => panic!("Not a row"),
    };

    let name: &str = row.get(1).unwrap();
    assert_eq!(name, "Max");

    Ok(())
}
