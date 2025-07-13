use std::{fs};
use std::collections::HashSet;
use serde::Deserialize;
use dobbydb_common_base::error::DobbyDBError;

#[derive(Debug, Clone, Deserialize)]
struct IcebergCatalogDefinition {
    name: String,
    url: String
}

#[derive(Debug, Clone, Deserialize)]
struct HiveCatalogDefinition {
    name: String,
    hms_url: String
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

pub struct CatalogManager {
    pub catalog_definitions: Vec<CatalogDefinition>
}

impl CatalogManager {
    pub fn new() -> Self {
        CatalogManager {
            catalog_definitions: Vec::new()
        }
    }
    pub fn init(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let toml_str = fs::read_to_string("../../../config/catalog.toml")?;
        let catalog_configs: CatalogConfigs = toml::from_str(&toml_str)?;
        let name_set: HashSet<String> = HashSet::new();

        for each in &catalog_configs.hive {
            if name_set.contains(&each.name) {
                return Err(Box::new(DobbyDBError::InvalidArgument("duplicate catalog name".to_string())));
            }
            self.catalog_definitions.push(CatalogDefinition::Hive(each.clone()));
        }

        dbg!("print catalog: {:#?}", catalog_configs);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_iceberg_catalog_definition() {
        let mut catalog_manager = CatalogManager::new();
        catalog_manager.init().expect("Failed to init catalog manager");
        // assert!(catalog_manager.init().is_ok());
    }
}

