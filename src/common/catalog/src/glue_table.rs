use crate::catalog_config::{DobbyCatalogConfig, GlueCatalogConfig};
use crate::table_format::external_table::{
    ExternalIcebergTable, ExternalTable, ExternalTableFormat,
};
use crate::table_format::table::TableIdentifier;
use async_trait::async_trait;
use aws_sdk_glue::types::Table;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::not_impl_err;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use dobbydb_common_base::config_key::ICEBERG_METADATA_LOCATION;
use iceberg::arrow::schema_to_arrow_schema;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct GlueTable {
    table_identifier: TableIdentifier,
    table_location: String,
    schema: SchemaRef,
    table_type: TableType,
    catalog_config: GlueCatalogConfig,
    external_table: ExternalTable,
}

impl GlueTable {
    pub async fn try_new(
        table_identifier: TableIdentifier,
        glue_table: &Table,
        catalog_config: &GlueCatalogConfig,
    ) -> Result<Self, DataFusionError> {
        let (table_format, table_location) = deduce_table_format(&glue_table.parameters)?;

        return match table_format {
            ExternalTableFormat::Iceberg => {
                let file_io_properties = catalog_config.build_iceberg_file_io_parameters();
                let table = ExternalIcebergTable::try_new(
                    &table_identifier,
                    &table_location,
                    file_io_properties,
                )
                .await?;
                let schema = schema_to_arrow_schema(table.static_table.metadata().current_schema())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(GlueTable {
                    table_identifier,
                    table_location,
                    schema: Arc::new(schema),
                    table_type: TableType::Base,
                    catalog_config: catalog_config.clone(),
                    external_table: ExternalTable::Iceberg(table),
                })
            }
            _ => Err(DataFusionError::NotImplemented(
                "not yet implemented".to_string(),
            )),
        };
    }
}

fn deduce_table_format(
    parameters: &Option<HashMap<String, String>>,
) -> Result<(ExternalTableFormat, String), DataFusionError> {
    if let Some(parameters) = parameters {
        let metadata = parameters.get(ICEBERG_METADATA_LOCATION).map(|v| v.clone());
        if let Some(metadata) = metadata {
            Ok((ExternalTableFormat::Iceberg, metadata))
        } else {
            Err(DataFusionError::Configuration(
                "invalid table parameters".to_string(),
            ))
        }
    } else {
        Err(DataFusionError::Configuration(
            "invalid table parameters".to_string(),
        ))
    }
}

#[async_trait]
impl TableProvider for GlueTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        self.table_type
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("not impl")
    }
}
