use crate::catalog_config::GlueCatalogConfig;
use crate::glue_catalog::GlueCatalog;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use datafusion::error::DataFusionError;
use serde::Deserialize;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
struct DobbyCatalogConfigs {
    glue: Vec<GlueCatalogConfig>,
}

#[derive(Debug)]
pub struct DobbyCatalogManager {
    catalogs: HashMap<String, Arc<dyn CatalogProvider>>,
}

impl DobbyCatalogManager {
    pub fn new() -> Self {
        DobbyCatalogManager {
            catalogs: HashMap::new(),
        }
    }
    pub async fn init_from_path(&mut self, config_path: &str) -> Result<(), DataFusionError> {
        let toml_str = fs::read_to_string(config_path)?;
        let catalog_configs: DobbyCatalogConfigs =
            toml::from_str(&toml_str).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut name_set: HashSet<String> = HashSet::new();

        for glue_config in &catalog_configs.glue {
            println!("start to load catalog config: {:?}", glue_config);
            if name_set.contains(&glue_config.name) {
                return Err(DataFusionError::Configuration(
                    "duplicate catalog name".to_string(),
                ));
            }
            let glue_catalog = GlueCatalog::try_new(&glue_config).await?;
            self.catalogs
                .insert(glue_config.name.clone(), Arc::new(glue_catalog));
            name_set.insert(glue_config.name.clone());
        }
        Ok(())
    }
}

impl CatalogProviderList for DobbyCatalogManager {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
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
