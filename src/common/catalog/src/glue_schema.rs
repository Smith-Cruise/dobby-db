use std::sync::Arc;
use async_trait::async_trait;
use aws_sdk_glue::types::Database;
use datafusion::catalog::{AsyncSchemaProvider, SchemaProvider, TableProvider};
use datafusion::error::DataFusionError;

pub struct GlueSchema {
    name: String,
    glue_database: Database,
    glue_client: Arc<aws_sdk_glue::Client>
}

impl GlueSchema {
    pub fn new(name: String, glue_database: Database, glue_client: Arc<aws_sdk_glue::Client>) -> Self {
        GlueSchema {
            name,
            glue_database,
            glue_client
        }
    }
}

#[async_trait]
impl AsyncSchemaProvider for GlueSchema {
    async fn table(&self, name: &str) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let resp = self.glue_client.get_table().database_name(name.clone()).name(name).send().await.map_err(|e| DataFusionError::External(Box::new(e)))?;
        match resp.table { 
            Some(table) => {
                
            },
            None => {
                Ok(None)
            }
        }
    }
}