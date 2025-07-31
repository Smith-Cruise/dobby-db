use std::any::Any;
use crate::glue_schema::GlueSchema;
use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_glue::config::Credentials;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider, CatalogProvider, SchemaProvider};
use datafusion::error::DataFusionError;
use dobbydb_common_base::config_key::{AWS_GLUE_ACCESS_KEY, AWS_GLUE_REGION, AWS_GLUE_SECRET_KEY};
use dobbydb_common_base::error::DobbyDBError;
use std::collections::HashMap;
use std::sync::Arc;
use crate::catalog::GlueCatalogProperties;

#[derive(Debug)]
pub struct GlueCatalog {
    glue_client: aws_sdk_glue::Client,
    config: GlueCatalogProperties,
    schemas: HashMap<String, Arc<dyn SchemaProvider>>
}

impl GlueCatalog {
    pub async fn try_new(properties: GlueCatalogProperties) -> Result<Self, DataFusionError> {
        let mut aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let (Some(access_key), Some(secret_key)) = (&properties.aws_glue_access_key, &properties.aws_glue_secret_key) {
            let credential_provider =
                Credentials::new(access_key, secret_key, None, None, "DobbyDB");
            aws_config = aws_config.credentials_provider(credential_provider);
        }
        if let Some(region) = &properties.aws_glue_region {
            aws_config = aws_config.region(Region::new(region.clone()));
        }
        let aws_config = aws_config.load().await;
        let glue_client = aws_sdk_glue::Client::new(&aws_config);
        let mut total_schemas: HashMap<String, Arc<dyn SchemaProvider>> = HashMap::new();
        let list_schemas_resp = glue_client.list_schemas().send().await.map_err(|e| DataFusionError::External(Box::new(e)))?;
        if let Some(schemas) = list_schemas_resp.schemas {
            for scheme in schemas {
                let schema_name = scheme.schema_name.unwrap_or_else(|| "default".to_string());
                let glue_schema = GlueSchema::try_new(&glue_client, schema_name.clone()).await?;
                total_schemas.insert(schema_name.clone(), Arc::new(glue_schema));
            }
        }
        Ok(GlueCatalog {
            glue_client: glue_client,
            config: properties,
            schemas: total_schemas,
        })
    }
}

// #[async_trait]
// impl AsyncCatalogProvider for GlueCatalog {
//     async fn schema(
//         &self,
//         name: &str,
//     ) -> datafusion::common::Result<Option<Arc<dyn AsyncSchemaProvider>>> {
//         let request_builder = self.glue_client.get_database().name(name);
//         let resp = request_builder
//             .send()
//             .await
//             .map_err(|e| DataFusionError::External(Box::new(e)))?;
//         match resp.database {
//             Some(database) => Ok(Some(Arc::new(GlueSchema::new(
//                 database.name.clone(),
//                 database,
//                 self.glue_client.clone(),
//             )))),
//             None => Ok(None),
//         }
//     }
// }

impl CatalogProvider for GlueCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}


