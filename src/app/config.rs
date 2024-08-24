use std::{collections::HashMap, path::PathBuf};

use directories::{ProjectDirs, UserDirs};
use lazy_static::lazy_static;
use serde::Deserialize;

lazy_static! {
    pub static ref PROJECT_NAME: String = env!("CARGO_CRATE_NAME").to_uppercase().to_string();
    pub static ref DATA_FOLDER: Option<PathBuf> =
        std::env::var(format!("{}_DATA", PROJECT_NAME.clone()))
            .ok()
            .map(PathBuf::from);
    pub static ref LOG_ENV: String = format!("{}_LOGLEVEL", PROJECT_NAME.clone());
    pub static ref LOG_FILE: String = format!("{}.log", env!("CARGO_PKG_NAME"));
}

fn project_directory() -> PathBuf {
    if let Some(user_dirs) = UserDirs::new() {
        return user_dirs.home_dir().join(".config").join("dft");
    };

    let maybe_project_dirs = ProjectDirs::from("", "", env!("CARGO_PKG_NAME"));
    if let Some(project_dirs) = maybe_project_dirs {
        project_dirs.data_local_dir().to_path_buf()
    } else {
        panic!("No known data directory")
    }
}

pub fn get_data_dir() -> PathBuf {
    if let Some(data_dir) = DATA_FOLDER.clone() {
        data_dir
    } else {
        project_directory()
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_datafusion_config")]
    pub datafusion: DataFusionConfig,
    #[serde(default = "default_display_config")]
    pub display: DisplayConfig,
    #[serde(default = "default_interaction_config")]
    pub interaction: InteractionConfig,
}

fn default_datafusion_config() -> DataFusionConfig {
    DataFusionConfig::default()
}

fn default_display_config() -> DisplayConfig {
    DisplayConfig::default()
}

fn default_interaction_config() -> InteractionConfig {
    InteractionConfig::default()
}

#[derive(Debug, Deserialize)]
pub struct DisplayConfig {
    #[serde(default = "default_tick_rate")]
    pub tick_rate: f64,
    #[serde(default = "default_frame_rate")]
    pub frame_rate: f64,
}

fn default_tick_rate() -> f64 {
    5.0
}

fn default_frame_rate() -> f64 {
    5.0
}

impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            tick_rate: 5.0,
            frame_rate: 5.0,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct DataFusionConfig {
    #[serde(default = "default_stream_batch_size")]
    pub stream_batch_size: usize,
}

fn default_stream_batch_size() -> usize {
    1
}

impl Default for DataFusionConfig {
    fn default() -> Self {
        Self {
            stream_batch_size: 1,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct InteractionConfig {
    #[serde(default = "default_mouse")]
    pub mouse: bool,
    #[serde(default = "default_paste")]
    pub paste: bool,
}

fn default_mouse() -> bool {
    false
}

fn default_paste() -> bool {
    false
}

fn default_fetch_limit() -> usize {
    100_usize
}
