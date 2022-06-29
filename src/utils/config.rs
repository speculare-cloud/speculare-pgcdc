use crate::Args;

use clap::Parser;
use config::ConfigError;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]

pub struct Config {
    // POSTGRESQL DB CONFIGS
    pub database_host: String,
    pub database_dbname: String,
    pub database_user: String,
    pub database_password: String,
    #[serde(default = "default_dbtls")]
    pub database_tls: bool,

    #[serde(default = "default_lookup_table")]
    pub lookup_table: HashMap<usize, String>,

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

fn default_lookup_table() -> HashMap<usize, String> {
    HashMap::from([
        (0, "disks".into()),
        (1, "cputimes".into()),
        (2, "cpustats".into()),
        (3, "ioblocks".into()),
        (4, "loadavg".into()),
        (5, "memory".into()),
        (6, "swap".into()),
        (7, "ionets".into()),
    ])
}
