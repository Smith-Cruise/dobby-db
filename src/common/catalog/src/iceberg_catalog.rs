use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::error::DataFusionError;
use crate::iceberg_schema::IcebergSchema;

#[derive(Debug)]
pub struct IcebergCatalog {
    pub schemas: HashMap<String, Arc<dyn SchemaProvider>>
}

impl IcebergCatalog {
    pub fn try_new() -> Result<Self, DataFusionError> {
        let mut iceberg_catalog = IcebergCatalog {
            schemas: HashMap::new()
        };
        iceberg_catalog.schemas.insert("db1".to_string(), Arc::new(IcebergSchema::try_new()?));
        Ok(iceberg_catalog)
    }
}

impl CatalogProvider for IcebergCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().map(|k| k.clone()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if let Some(schema) = self.schemas.get(name) {
            Some(schema.clone())
        } else {
            None
        }
    }
}