#[cfg(feature = "e2e")]
use postgres::{Client, Error, NoTls};
use std::thread;

#[test]
#[cfg(feature = "e2e")]
fn test_end_to_end() -> Result<(), Error> {
    thread::spawn(|| {
        // Launch proxy
        let app = proboscis::new("0.0.0.0:5432");
        app.listen("0.0.0.0:5430");
    });

    let mut client = Client::connect("host=0.0.0.0 port=5430 user=admin password=password", NoTls)?;

    let result = client.query_one("SELECT id, name FROM person", &[])?;
    let name: &str = result.get(1);

    assert_eq!(name, "Max");

    Ok(())
}
