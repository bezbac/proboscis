use url::Url;

#[derive(Clone, Debug)]
pub struct TargetConfig {
    pub host: String,
    pub port: u16,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl TargetConfig {
    pub fn from_uri(input: &str) -> Result<TargetConfig, String> {
        let url = Url::parse(input).map_err(|err| err.to_string())?;

        if !(url.scheme() == "postgres" || url.scheme() == "postgresql") {
            return Err("uri doesnt't start with 'postgres' or 'postgresql'".to_string());
        }

        let host = match url.host() {
            Some(host) => host.to_string(),
            _ => "0.0.0.0".to_string(),
        };

        let port = match url.port() {
            Some(port) => port,
            _ => 5432,
        };

        let database = url
            .path_segments()
            .iter()
            .next()
            .map(|database| database.clone().collect::<String>());

        let user = if !url.username().is_empty() {
            Some(url.username().to_string())
        } else {
            None
        };

        let password = url.password().map(|password| password.to_string());

        let config = TargetConfig {
            host,
            port,
            database,
            user,
            password,
        };

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_uri_full() {
        let target_config =
            TargetConfig::from_uri("postgres://admin:password@0.0.0.0:5438/postgres").unwrap();

        assert_eq!(target_config.host, "0.0.0.0");
        assert_eq!(target_config.port, 5438);
        assert_eq!(target_config.user, Some("admin".to_string()));
        assert_eq!(target_config.password, Some("password".to_string()));
        assert_eq!(target_config.database, Some("postgres".to_string()));
    }

    #[test]
    fn test_config_from_uri_various() {
        let uris = vec![
            "postgresql://",
            "postgresql://localhost",
            "postgresql://localhost:5432",
            "postgresql://localhost/mydb",
            "postgresql://user@localhost",
            "postgresql://user:secret@localhost",
            "postgresql://localhost/mydb?user=other&password=secret",
        ];

        for uri in uris {
            TargetConfig::from_uri(uri).unwrap();
        }
    }
}
