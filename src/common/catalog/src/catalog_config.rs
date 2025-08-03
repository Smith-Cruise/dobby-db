use std::collections::HashMap;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_REGION, S3_SECRET_ACCESS_KEY};
use serde::Deserialize;

pub trait DobbyCatalogConfig {
    fn build_iceberg_file_io_parameters(&self) -> HashMap<String, String>;
}

#[derive(Debug, Clone, Deserialize)]
pub struct GlueCatalogConfig {
    pub name: String,
    #[serde(rename = "aws-glue-region")]
    pub aws_glue_region: Option<String>,
    #[serde(rename = "aws-glue-access-key")]
    pub aws_glue_access_key: Option<String>,
    #[serde(rename = "aws-glue-secret-key")]
    pub aws_glue_secret_key: Option<String>,
    #[serde(rename = "aws-s3-region")]
    pub aws_s3_region: Option<String>,
    #[serde(rename = "aws-s3-access-key")]
    pub aws_s3_access_key: Option<String>,
    #[serde(rename = "aws-s3-secret-key")]
    pub aws_s3_secret_key: Option<String>,
}

impl DobbyCatalogConfig for GlueCatalogConfig {
    fn build_iceberg_file_io_parameters(&self) -> HashMap<String, String> {
        let mut map: HashMap<String, String> = HashMap::new();
        if let Some(region) = &self.aws_s3_region {
            map.insert(S3_REGION.into(), region.clone());
        }
        if let Some(access_key) = &self.aws_s3_access_key {
            map.insert(S3_ACCESS_KEY_ID.into(), access_key.clone());
        }
        if let Some(secret_key) = &self.aws_s3_secret_key {
            map.insert(S3_SECRET_ACCESS_KEY.into(), secret_key.clone());
        }
        map
    }
}