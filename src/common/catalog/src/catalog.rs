use std::{fs};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::common::cse::FoundCommonNodes::No;
use datafusion::common::not_impl_err;
use datafusion::error::DataFusionError;
use serde::Deserialize;
use dobbydb_common_base::error::DobbyDBError;
use crate::glue_catalog::GlueCatalog;
use crate::iceberg_catalog::IcebergCatalog;


#[derive(Debug, Clone, Deserialize)]
pub struct GlueCatalogProperties {
    pub name: String,
    #[serde(rename = "aws-glue-region")]
    pub aws_glue_region: Option<String>,
    #[serde(rename = "aws-glue-access-key")]
    pub aws_glue_access_key: Option<String>,
    #[serde(rename = "aws-glue-secret-key")]
    pub aws_glue_secret_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CatalogConfigs {
    glue: Vec<GlueCatalogProperties>,
}

#[derive(Debug)]
pub struct DobbyDBCatalogManager {
    pub catalogs: HashMap<String, Arc<dyn CatalogProvider>>
}

impl DobbyDBCatalogManager {
    pub fn new() -> Self {
        DobbyDBCatalogManager {
            catalogs: HashMap::new()
        }
    }
    pub fn init_from_path(&mut self, config_path: &str) -> Result<(), DataFusionError> {
        let toml_str = fs::read_to_string(config_path)?;
        let catalog_configs: CatalogConfigs = toml::from_str(&toml_str)?;
        let mut name_set: HashSet<String> = HashSet::new();


        for each in &catalog_configs.glue {
            if name_set.contains(&each.name) {
                return Err(DataFusionError::Configuration("duplicate catalog name".to_string()));
            }
            let glue_catalog = GlueCatalog::try_new(each.clone()).await?;
            self.catalogs.insert("iceberg_test".to_string(), Arc::new(iceberg_catalog));
            name_set.insert(each.name.clone());
        }

        Ok(())
    }
}

impl CatalogProviderList for DobbyDBCatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(&self, name: String, catalog: Arc<dyn CatalogProvider>) -> Option<Arc<dyn CatalogProvider>> {
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.keys().map(|k| k.clone()).collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if let Some(catalog) = self.catalogs.get(name) {
            Some(catalog.clone())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_iceberg_catalog_definition() {
        // let mut catalog_manager = CatalogManager::new();
        // catalog_manager.init("x").expect("Failed to init catalog manager");
        // assert!(catalog_manager.init().is_ok());
    }
}

