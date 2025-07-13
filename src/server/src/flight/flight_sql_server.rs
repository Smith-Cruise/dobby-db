use arrow_array::{RecordBatch, StringArray};
use std::sync::Arc;
use tonic::{Request, Response, Status};

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    CommandGetCatalogs
    , ProstMessageExt, SqlInfo
};
use arrow_flight::{
    flight_service_server::FlightService, FlightDescriptor, FlightEndpoint,
    FlightInfo,
    Ticket,
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use prost::Message;
use tonic::codegen::Bytes;

#[derive(Clone)]
pub struct DobbyDBFlightService {}

#[tonic::async_trait]
impl FlightSqlService for DobbyDBFlightService {
    type FlightService = Self;

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let catalog_schema = Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]);
        let ticket = Ticket {
            ticket: Bytes::from(query.as_any().encode_to_vec()),
        };

        // 2. 创建 FlightEndpoint（包含 Ticket 和服务器地址）
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let flight_info = FlightInfo::new()
            .with_total_bytes(-1)
            .with_total_records(-1)
            .with_ordered(false)
            .with_endpoint(endpoint)
            .try_with_schema(&catalog_schema)
            .map_err(|err| Status::internal(format!("{err:?}")))?;
        println!("get flight info catalogs");
        Ok(Response::new(flight_info))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!("ticket is {}", request.into_inner().to_string());
        let catalog_schema = Arc::new(Schema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            false,
        )]));
        let catalogs = StringArray::from(vec!["default", "system"]);
        let batch = RecordBatch::try_new(Arc::clone(&catalog_schema), vec![Arc::new(catalogs)])
            .map_err(|e| Status::internal(e.to_string()))?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(Arc::clone(&catalog_schema))
            .build(futures::stream::once(async { Ok(batch) }))
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, id: i32, _result: &SqlInfo) {
        println!("{}", id);
    }
}