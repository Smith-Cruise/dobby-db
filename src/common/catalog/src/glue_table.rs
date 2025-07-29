use std::any::Any;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::not_impl_err;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

#[derive(Debug)]
struct GlueTable {
    name: String,
    glue_table: GlueTable,
    schema: SchemaRef,
    table_type: TableType,
}

impl GlueTable {

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