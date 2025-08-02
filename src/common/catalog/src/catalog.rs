use std::{fs};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use datafusion::error::DataFusionError;
use serde::Deserialize;
use crate::glue_catalog::GlueCatalog;


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
    // catalogs: HashMap<String, Arc<dyn AsyncCatalogProvider>>
    catalogs: HashMap<String, Arc<dyn CatalogProvider>>
}

impl DobbyDBCatalogManager {
    pub fn new() -> Self {
        DobbyDBCatalogManager {
            catalogs: HashMap::new()
        }
    }
    pub async fn init_from_path(&mut self, config_path: &str) -> Result<(), DataFusionError> {
        let toml_str = fs::read_to_string(config_path)?;
        let catalog_configs: CatalogConfigs = toml::from_str(&toml_str).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut name_set: HashSet<String> = HashSet::new();

        for each in &catalog_configs.glue {
            println!("start to load catalog config: {:?}", each);
            if name_set.contains(&each.name) {
                return Err(DataFusionError::Configuration("duplicate catalog name".to_string()));
            }
            let glue_catalog = GlueCatalog::try_new(&each).await?;
            self.catalogs.insert(each.name.clone(), Arc::new(glue_catalog));
            name_set.insert(each.name.clone());
        }
        Ok(())
    }
}

// #[async_trait]
// impl AsyncCatalogProviderList for DobbyDBCatalogManager {
//     async fn catalog(&self, name: &str) -> datafusion::common::Result<Option<Arc<dyn AsyncCatalogProvider>>> {
//         let catalog = self.catalogs.get(name).cloned();
//         Ok(catalog)
//     }
// }

impl CatalogProviderList for DobbyDBCatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(&self, name: String, catalog: Arc<dyn CatalogProvider>) -> Option<Arc<dyn CatalogProvider>> {
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.keys().cloned().collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let catalog = self.catalogs.get(name)?;
        Some(catalog.clone())
    }
}

#[cfg(test)]
mod tests {
    

    #[test]
    fn test_parse_iceberg_catalog_definition() {
        // let mut catalog_manager = CatalogManager::new();
        // catalog_manager.init("x").expect("Failed to init catalog manager");
        // assert!(catalog_manager.init().is_ok());
    }
}

