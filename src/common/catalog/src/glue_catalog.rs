use crate::glue_schema::GlueSchema;
use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_glue::config::Credentials;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider, SchemaProvider};
use datafusion::error::DataFusionError;
use dobbydb_common_base::config_key::{AWS_GLUE_ACCESS_KEY, AWS_GLUE_REGION, AWS_GLUE_SECRET_KEY};
use dobbydb_common_base::error::DobbyDBError;
use std::collections::HashMap;
use std::sync::Arc;

struct GlueCatalogConfig {
    access_key: Option<String>,
    secret_key: Option<String>,
    region: Option<String>,
}

impl GlueCatalogConfig {
    pub fn new(properties: &HashMap<String, String>) -> Self {
        GlueCatalogConfig {
            access_key: properties.get(AWS_GLUE_ACCESS_KEY).cloned(),
            secret_key: properties.get(AWS_GLUE_SECRET_KEY).cloned(),
            region: properties.get(AWS_GLUE_REGION).cloned(),
        }
    }
}

struct GlueCatalog {
    glue_client: Arc<aws_sdk_glue::Client>,
    config: GlueCatalogConfig,
}

impl GlueCatalog {
    pub async fn try_new(properties: &HashMap<String, String>) -> Result<Self, DobbyDBError> {
        let config = GlueCatalogConfig::new(properties);
        let mut aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let (Some(access_key), Some(secret_key)) = (&config.access_key, &config.secret_key) {
            let credential_provider =
                Credentials::new(access_key, secret_key, None, None, "DobbyDB");
            aws_config = aws_config.credentials_provider(credential_provider);
        }
        if let Some(region) = &config.region {
            aws_config = aws_config.region(Region::new(region.clone()));
        }
        let aws_config = aws_config.load().await;
        let glue_client = aws_sdk_glue::Client::new(&aws_config);
        Ok(GlueCatalog {
            glue_client: Arc::new(glue_client),
            config: config,
        })
    }
}

#[async_trait]
impl AsyncCatalogProvider for GlueCatalog {
    async fn schema(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        let request_builder = self.glue_client.get_database().name(name);
        let resp = request_builder
            .send()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        match resp.database {
            Some(database) => Ok(Some(Arc::new(GlueSchema::new(
                database.name.clone(),
                database,
                self.glue_client.clone(),
            )))),
            None => Ok(None),
        }
    }
}
