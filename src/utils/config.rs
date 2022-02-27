use crate::Args;

use clap::Parser;
use config::ConfigError;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]

pub struct Config {
    // POSTGRESQL DB CONFIGS
    pub database_host: String,
    pub database_dbname: String,
    pub database_user: String,
    pub database_password: String,
    #[serde(default = "default_dbtls")]
    pub database_tls: bool,

    // HTTP API CONFIGS
    #[serde(default = "default_https")]
    pub https: bool,
    pub key_priv: Option<String>,
    pub key_cert: Option<String>,
    #[serde(default = "default_binding")]
    pub binding: String,
}

impl Config {
    pub fn new() -> Result<Self, ConfigError> {
        let args = Args::parse();

        let config_builder = config::Config::builder().add_source(config::File::new(
            &args
                .config_path
                .unwrap_or_else(|| "/etc/speculare/pgcdc.config".to_owned()),
            config::FileFormat::Toml,
        ));

        config_builder.build()?.try_deserialize()
    }
}

fn default_https() -> bool {
    false
}

fn default_dbtls() -> bool {
    false
}

fn default_binding() -> String {
    String::from("0.0.0.0:8080")
}
