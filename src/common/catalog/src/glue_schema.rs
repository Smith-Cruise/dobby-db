use std::any::Any;
use std::collections::HashMap;
use crate::glue_table::GlueTable;
use async_trait::async_trait;
use aws_sdk_glue::types::Database;
use datafusion::catalog::{AsyncSchemaProvider, SchemaProvider, TableProvider};
use datafusion::error::DataFusionError;
use std::sync::Arc;

#[derive(Debug)]
pub struct GlueSchema {
    schema_name: String,
    tables: HashMap<String, Arc<dyn TableProvider>>,
    //glue_database: Database,
    //glue_client: Arc<aws_sdk_glue::Client>,
}

impl GlueSchema {
    pub async fn try_new(
        glue_client: &aws_sdk_glue::Client,
        schema_name: String,
    ) -> Result<Self, DataFusionError> {
        let mut hash_tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        let resp = glue_client.get_tables().database_name(schema_name.clone()).send().await.map_err(|e| DataFusionError::External(Box::new(e)))?;
        if let Some(tables) = resp.table_list {
            for table in tables {
                let table_name = table.name.clone();
                let build_table = GlueTable::try_new(table_name.clone(), table)?;
                hash_tables.insert(table_name.clone(), Arc::new(build_table));
            }
        }
        Ok(GlueSchema {
            schema_name: schema_name.clone(),
            tables: hash_tables,
        })
    }
}

#[async_trait]
impl SchemaProvider for GlueSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

// #[async_trait]
// impl AsyncSchemaProvider for GlueSchema {
//     async fn table(
//         &self,
//         name: &str,
//     ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
//         let resp = self
//             .glue_client
//             .get_table()
//             .database_name(self.schema_name.clone())
//             .name(name)
//             .send()
//             .await
//             .map_err(|e| DataFusionError::External(Box::new(e)))?;
//         match resp.table {
//             Some(table) => Ok(Some(Arc::new(GlueTable::try_new(
//                 table.name.clone(),
//                 table,
//             )?))),
//             None => Ok(None),
//         }
//     }
// }
