use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;

mod flight;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::]:8081".parse()?;
    let service = flight::flight_sql_server::DobbyDBFlightService {};

    let svc = FlightServiceServer::new(service);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}