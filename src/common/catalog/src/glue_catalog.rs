use crate::catalog_config::GlueCatalogConfig;
use crate::glue_schema::GlueDatabase;
use aws_config::Region;
use aws_sdk_glue::config::Credentials;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::error::DataFusionError;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct GlueCatalog {
    config: GlueCatalogConfig,
    databases: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl GlueCatalog {
    pub async fn try_new(catalog_config: &GlueCatalogConfig) -> Result<Self, DataFusionError> {
        let mut aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let (Some(access_key), Some(secret_key)) = (
            &catalog_config.aws_glue_access_key,
            &catalog_config.aws_glue_secret_key,
        ) {
            let credential_provider =
                Credentials::new(access_key, secret_key, None, None, "DobbyDB");
            aws_config = aws_config.credentials_provider(credential_provider);
        }
        if let Some(region) = &catalog_config.aws_glue_region {
            aws_config = aws_config.region(Region::new(region.clone()));
        }
        let aws_config = aws_config.load().await;
        let glue_client = aws_sdk_glue::Client::new(&aws_config);
        let mut total_databases: HashMap<String, Arc<dyn SchemaProvider>> = HashMap::new();
        let dbs = glue_client
            .get_databases()
            .send()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        for database in dbs.database_list {
            let glue_schema =
                GlueDatabase::try_new(&glue_client, &database.name, catalog_config).await?;
            total_databases.insert(database.name.clone(), Arc::new(glue_schema));
        }
        Ok(GlueCatalog {
            config: catalog_config.clone(),
            databases: total_databases,
        })
    }
}

impl CatalogProvider for GlueCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.databases.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.databases.get(name).cloned()
    }
}
