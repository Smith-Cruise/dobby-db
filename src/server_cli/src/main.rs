use clap::Parser;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_cli::exec;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::{MaxRows, PrintOptions};
use dobbydb_common_catalog::catalog::DobbyDBCatalogManager;
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::ContextModifier::Session;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(short, long)]
    config_path: String,

    #[clap(long, value_enum, default_value_t = PrintFormat::Automatic)]
    format: PrintFormat,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,

    #[clap(
        long,
        help = "The max number of rows to display for 'Table' format\n[possible values: numbers(0/10/...), inf(no limit)]",
        default_value = "40"
    )]
    maxrows: MaxRows,

    #[clap(long, help = "Enables console syntax highlighting")]
    color: bool,
}

#[tokio::main]
async fn main() {
    //run_from_command().await.unwrap();

    let ctx = SessionContext::new();
    let sql = "SELECT * from remote_schema.remote_table";
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect.as_str();
    let statement = state.sql_to_statement(sql, dialect).unwrap();
    let references = state.resolve_table_references(&statement).unwrap();
    // ctx.register_cat
}

async fn run_from_command() -> Result<(), DataFusionError> {
    let args = Args::parse();
    let mut catalog_manager = DobbyDBCatalogManager::new();
    catalog_manager.init_from_path(args.config_path.as_str()).await?;
    let config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("iceberg", "p");
    let ctx = SessionContext::new_with_config(config);

    // ctx.register_catalog_list(Arc::new(catalog_manager));
    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
        maxrows: args.maxrows,
        color: args.color,
    };
    println!("Hello, world!");

    Ok(exec::exec_from_repl(&ctx, &mut print_options).await.map_err(|e| DataFusionError::External(Box::new(e)))?)
}
