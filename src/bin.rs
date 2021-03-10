extern crate proboscis;

struct Config {
    app_host: String,
    app_port: String,

    target_host: String,
    target_port: String,
}

fn main() {
    let config: Config = Config {
        app_host: "0.0.0.0".to_string(),
        app_port: "5430".to_string(),

        target_host: "0.0.0.0".to_string(),
        target_port: "5432".to_string(),
    };

    let target_addr = format!("{}:{}", config.target_host, config.target_port);
    let app_addr = format!("{}:{}", config.app_host, config.app_port);

    let app = proboscis::new(&target_addr);
    app.listen(&app_addr);
}
