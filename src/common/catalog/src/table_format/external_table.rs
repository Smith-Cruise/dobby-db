use crate::table_format::table::TableIdentifier;
use datafusion::error::DataFusionError;
use iceberg::io::FileIO;
use iceberg::table::StaticTable;
use iceberg::{NamespaceIdent, TableIdent};
use std::collections::HashMap;

#[derive(Eq, Debug, Clone, PartialEq)]
pub enum ExternalTableFormat {
    Hive,
    Iceberg
}
#[derive(Debug)]
pub enum ExternalTable {
    Hive(ExternalHiveTable),
    Iceberg(ExternalIcebergTable),
    Invalid,
}

#[derive(Debug)]
pub struct ExternalHiveTable {

}

#[derive(Debug)]
pub struct ExternalIcebergTable {
    pub static_table: StaticTable,
}


impl ExternalIcebergTable {
    pub async fn try_new(
        table_identifier: &TableIdentifier,
        table_location: &str,
        file_io_config: HashMap<String, String>,
    ) -> Result<Self, DataFusionError> {
        let file_io = FileIO::from_path(table_location)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .with_props(file_io_config)
            .build()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let iceberg_identifier: TableIdent = TableIdent {
            namespace: NamespaceIdent::from_vec(table_identifier.namespace.clone())
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
            name: table_identifier.name.clone(),
        };

        let iceberg_table =
            StaticTable::from_metadata_file(table_location, iceberg_identifier, file_io)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            static_table: iceberg_table,
        })
    }
}
