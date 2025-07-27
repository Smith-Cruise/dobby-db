use std::{fs};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::common::cse::FoundCommonNodes::No;
use datafusion::common::not_impl_err;
use serde::Deserialize;
use dobbydb_common_base::error::DobbyDBError;
use crate::iceberg_catalog::IcebergCatalog;

#[derive(Debug, Clone, Deserialize)]
pub struct HiveCatalogProperties {
    pub name: String,
    pub hms_url: String
}
#[derive(Debug, Clone, Deserialize)]
pub struct IcebergCatalogProperties {
    pub name: String,
    pub url: String
}

#[derive(Debug, Deserialize)]
struct CatalogConfigs {
    hive: Vec<HiveCatalogProperties>,
    iceberg: Vec<IcebergCatalogProperties>
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
    pub fn init_from_path(&mut self, config_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let toml_str = fs::read_to_string(config_path)?;
        let catalog_configs: CatalogConfigs = toml::from_str(&toml_str)?;
        let mut name_set: HashSet<String> = HashSet::new();
        //
        // for each in &catalog_configs.hive {
        //     if name_set.contains(&each.name) {
        //         return Err(Box::new(DobbyDBError::InvalidArgument("duplicate catalog name".to_string())));
        //     }
        //     self.catalog_definitions.push(CatalogDefinition::Hive(each.clone()));
        //     name_set.insert(each.name.clone());
        // }

        for each in &catalog_configs.iceberg {
            if name_set.contains(&each.name) {
                return Err(Box::new(DobbyDBError::InvalidArgument("duplicate catalog name".to_string())));
            }
            let iceberg_catalog = IcebergCatalog::try_new()?;
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

