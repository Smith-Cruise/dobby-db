use arrow_array::{RecordBatch, StringArray};
use std::sync::{Arc, LazyLock};
use tonic::{Request, Response, Status};

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandGetCatalogs, ProstMessageExt, SqlInfo};
use arrow_flight::{
    flight_service_server::FlightService, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use prost::Message;
use tonic::codegen::Bytes;

#[derive(Clone)]
pub struct DobbyDBFlightService {}

static FLIGHT_CATALOG_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("catalog_type", DataType::Utf8, false),
    ])
});

#[tonic::async_trait]
impl FlightSqlService for DobbyDBFlightService {
    type FlightService = Self;

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
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
            .try_with_schema(&FLIGHT_CATALOG_SCHEMA)
            .map_err(|err| Status::internal(format!("{err:?}")))?;
        Ok(Response::new(flight_info))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        println!("ticket is {}", request.into_inner().to_string());
        let mut catalog_names: Vec<String> = Vec::new();
        let mut catalog_types: Vec<String> = Vec::new();
        // let catalog_definitions = &CATALOG_MANAGER.lock().unwrap().catalog_definitions;
        // for catalog_definition in catalog_definitions {
        //     match catalog_definition {
        //         CatalogDefinition::Iceberg(each) => {
        //             catalog_names.push(each.name.clone());
        //             catalog_types.push("Iceberg".to_string());
        //         }
        //         CatalogDefinition::Hive(each) => {
        //             catalog_names.push(each.name.clone());
        //             catalog_types.push("Hive".to_string());
        //         }
        //     }
        // }
        let batch = RecordBatch::try_new(
            Arc::new(FLIGHT_CATALOG_SCHEMA.clone()),
            vec![
                Arc::new(StringArray::from(catalog_names)),
                Arc::new(StringArray::from(catalog_types)),
            ],
        )
        .map_err(|e| Status::internal(e.to_string()))?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(Arc::new(FLIGHT_CATALOG_SCHEMA.clone()))
            .build(futures::stream::once(async { Ok(batch) }))
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, id: i32, _result: &SqlInfo) {
        println!("{}", id);
    }
}
