use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use aws_sdk_glue::types::Table;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::not_impl_err;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use dobbydb_common_base::config_key::ICEBERG_METADATA_LOCATION;

#[derive(Debug)]
pub struct GlueTable {
    name: String,
    table_location: String,
    schema: SchemaRef,
    table_type: TableType,
}

impl GlueTable {
    pub fn try_new(name: String, glue_table: Table) -> Result<Self, DataFusionError> {
        let table_location = get_metadata_location(&glue_table.parameters)?;
        let schema = Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("catalog_type", DataType::Utf8, false),
        ]);
        Ok(GlueTable {
            name,
            table_location,
            schema: SchemaRef::new(schema),
            table_type: TableType::Base
        })
    }
}

fn get_metadata_location(parameters: &Option<HashMap<String, String>>) -> Result<String, DataFusionError> {
    if let Some (parameters) = parameters {
        let metadata = parameters.get(ICEBERG_METADATA_LOCATION).map(|v| v.clone());
        if let Some(metadata) = metadata {
            Ok(metadata)
        } else {
            Err(DataFusionError::Configuration("invalid table parameters".to_string()))
        }
    } else {
        Err(DataFusionError::Configuration("invalid table parameters".to_string()))
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

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("not impl")
    }
}