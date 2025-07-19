use std::{fs};
use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};
use serde::Deserialize;
use dobbydb_common_base::error::DobbyDBError;

#[derive(Debug, Clone, Deserialize)]
pub struct IcebergCatalogDefinition {
    pub name: String,
    pub url: String
}

#[derive(Debug, Clone, Deserialize)]
pub struct HiveCatalogDefinition {
    pub name: String,
    pub hms_url: String
}

#[derive(Debug, Deserialize)]
struct CatalogConfigs {
    hive: Vec<HiveCatalogDefinition>,
    iceberg: Vec<IcebergCatalogDefinition>
}

pub enum CatalogDefinition {
    Iceberg(IcebergCatalogDefinition),
    Hive(HiveCatalogDefinition),
}

pub static CATALOG_MANAGER: LazyLock<Mutex<CatalogManager>> = LazyLock::new(|| {
    Mutex::new(CatalogManager::new())
});

pub struct CatalogManager {
    pub catalog_definitions: Vec<CatalogDefinition>
}

impl CatalogManager {
    pub fn new() -> Self {
        CatalogManager {
            catalog_definitions: Vec::new()
        }
    }
    pub fn init(&mut self, config_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let toml_str = fs::read_to_string(config_path)?;
        let catalog_configs: CatalogConfigs = toml::from_str(&toml_str)?;
        let mut name_set: HashSet<String> = HashSet::new();

        for each in &catalog_configs.hive {
            if name_set.contains(&each.name) {
                return Err(Box::new(DobbyDBError::InvalidArgument("duplicate catalog name".to_string())));
            }
            self.catalog_definitions.push(CatalogDefinition::Hive(each.clone()));
            name_set.insert(each.name.clone());
        }

        for each in &catalog_configs.iceberg {
            if name_set.contains(&each.name) {
                return Err(Box::new(DobbyDBError::InvalidArgument("duplicate catalog name".to_string())));
            }
            self.catalog_definitions.push(CatalogDefinition::Iceberg(each.clone()));
            name_set.insert(each.name.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_iceberg_catalog_definition() {
        let mut catalog_manager = CatalogManager::new();
        catalog_manager.init("x").expect("Failed to init catalog manager");
        // assert!(catalog_manager.init().is_ok());
    }
}

