use arrow_array::{RecordBatch, StringArray};
use futures::stream::BoxStream;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::sql::server::{DoPutError, FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
    ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
    ActionEndSavepointRequest, ActionEndTransactionRequest, Any, Command, CommandGetCatalogs,
    CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandGetXdbcTypeInfo, CommandPreparedStatementQuery, CommandPreparedStatementUpdate,
    CommandStatementIngest, CommandStatementQuery, CommandStatementSubstraitPlan,
    CommandStatementUpdate, DoPutPreparedStatementResult, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService,
};
use arrow_schema::{DataType, Field, Schema};
use futures::{Stream, TryStreamExt};
use tonic::codegen::Bytes;

#[derive(Clone)]
pub struct DobbyDBFlightService {}

#[tonic::async_trait]
impl FlightSqlService for DobbyDBFlightService {
    type FlightService = Self;

    async fn get_flight_info_catalogs(&self, _query: CommandGetCatalogs, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        let catalog_schema = Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]);
        let ticket = Ticket {
            ticket: Bytes::from(b"get_catalogs".to_vec()),
        };

        // 2. 创建 FlightEndpoint（包含 Ticket 和服务器地址）
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let flight_info = FlightInfo::new()
            .with_endpoint(endpoint)
            .try_with_schema(&catalog_schema)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(flight_info))
    }

    async fn do_get_catalogs(&self, _query: CommandGetCatalogs, _request: Request<Ticket>) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // 返回目录数据的 RecordBatch
        let catalog_schema = Arc::new(Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]));
        let catalogs = StringArray::from(vec!["default", "system"]);
        let batch = RecordBatch::try_new(
            Arc::clone(&catalog_schema),
            vec![Arc::new(catalogs)],
        ).map_err(|e| Status::internal(e.to_string()))?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(Arc::clone(&catalog_schema))
            .build(futures::stream::once(async { Ok(batch) }))
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, id: i32, result: &SqlInfo) {
        println!("{}", id);
    }
}

#[derive(Clone)]
pub struct FlightServiceImpl {}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // request.get_ref().r#type()
        let catalog_schema = Schema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]);
        let ticket = Ticket {
            ticket: Bytes::from(b"get_catalogs".to_vec()),
        };

        // 2. 创建 FlightEndpoint（包含 Ticket 和服务器地址）
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let flight_info = FlightInfo::new()
            .with_endpoint(endpoint)
            .try_with_schema(&catalog_schema)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(flight_info))
    }
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.get_ref();
        if ticket.ticket == Bytes::from("get_catalogs") {
            // 返回目录数据的 RecordBatch
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
        } else {
            Err(Status::unimplemented("Implement do_get"))
        }
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Implement do_put"))
    }

    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }

    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }
}
