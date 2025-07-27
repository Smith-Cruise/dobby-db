use std::any::Any;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::not_impl_err;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

#[derive(Debug)]
pub struct IcebergTable {
    pub name: String
}

impl IcebergTable {
    pub fn try_new(table_name: &str) -> Result<IcebergTable, DataFusionError> {
        let iceberg_table = IcebergTable {
            name: table_name.to_string(),
        };
        Ok(iceberg_table)
    }
}

#[async_trait]
impl TableProvider for IcebergTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("not implement")
    }
}