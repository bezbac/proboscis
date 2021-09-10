use anyhow::Result;
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
    pub fn from_uri(input: &str) -> Result<TargetConfig> {
        let url = Url::parse(input)?;

        anyhow::ensure!(
            url.scheme() == "postgres" || url.scheme() == "postgresql",
            "uri doesnt't start with 'postgres' or 'postgresql'"
        );

        let host = match url.host() {
            Some(host) => host.to_string(),
            _ => "0.0.0.0".to_string(),
        };

        let port = match url.port() {
            Some(port) => port,
            _ => 5432,
        };

        let database = match url.path_segments().iter().next() {
            Some(database) => Some(database.clone().collect::<String>()),
            _ => None,
        };

        let user = if url.username().len() > 0 {
            Some(url.username().to_string())
        } else {
            None
        };

        let password = match url.password() {
            Some(password) => Some(password.to_string()),
            _ => None,
        };

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
