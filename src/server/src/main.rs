use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use tonic::transport::Server;

mod flight;
mod parser;

#[derive(Parser, Debug)]
#[command(version, about)]
struct DobbyDBServerConfig {
    #[arg(short, long)]
    config_path: String
}

struct DobbyDBServer {
}

impl DobbyDBServer {
    pub fn new() -> Self {
        DobbyDBServer {
        }
    }

    pub fn init(&mut self, config: DobbyDBServerConfig) -> Result<(), Box<dyn std::error::Error>> {
        // CATALOG_MANAGER.lock().unwrap().init(&config.config_path)?;
        Ok(())
    }
    
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = "[::]:8081".parse()?;
        let service = flight::flight_sql_server::DobbyDBFlightService {};

        let svc = FlightServiceServer::new(service);

        Server::builder().add_service(svc).serve(addr).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_config = DobbyDBServerConfig::parse();
    let mut dobbydb_server = DobbyDBServer::new();
    dobbydb_server.init(server_config)?;
    dobbydb_server.run().await?;
    Ok(())
}