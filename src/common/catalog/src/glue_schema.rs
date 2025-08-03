use crate::catalog_config::GlueCatalogConfig;
use crate::glue_table::GlueTable;
use crate::table_format::table::TableIdentifier;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::DataFusionError;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct GlueDatabase {
    database_name: String,
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl GlueDatabase {
    pub async fn try_new(
        glue_client: &aws_sdk_glue::Client,
        database_name: &str,
        glue_config: &GlueCatalogConfig,
    ) -> Result<Self, DataFusionError> {
        let mut hash_tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        let resp = glue_client
            .get_tables()
            .database_name(database_name)
            .send()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if let Some(tables) = resp.table_list {
            for table in tables {
                let build_table = GlueTable::try_new(
                    TableIdentifier::new(database_name, &table.name),
                    &table,
                    glue_config,
                )
                .await?;
                hash_tables.insert(table.name.clone().clone(), Arc::new(build_table));
            }
        }
        Ok(GlueDatabase {
            database_name: database_name.to_string(),
            tables: hash_tables,
        })
    }
}

#[async_trait]
impl SchemaProvider for GlueDatabase {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
