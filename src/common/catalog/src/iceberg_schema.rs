use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use crate::iceberg_table::IcebergTable;

#[derive(Debug)]
pub struct IcebergSchema {
    pub tables: HashMap<String, Arc<IcebergTable>>
}

impl IcebergSchema {
    pub fn try_new() -> Result<IcebergSchema, DataFusionError> {
        let mut iceberg_database = IcebergSchema {
            tables: HashMap::new()
        };
        
        let table_1 = Arc::new(IcebergTable::try_new("table_1")?);
        let table_2 = Arc::new(IcebergTable::try_new("table_2")?);
        iceberg_database.tables.insert(table_1.name.clone(), table_1.clone());
        iceberg_database.tables.insert(table_2.name.clone(), table_2.clone());
        Ok(iceberg_database)
    }
}

#[async_trait]
impl SchemaProvider for IcebergSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().map(|k| k.clone()).collect()
    }

    async fn table(&self, name: &str) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if let Some(table) = self.tables.get(name) {
            Ok(Some(table.clone()))   
        } else {
            Ok(None)
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}