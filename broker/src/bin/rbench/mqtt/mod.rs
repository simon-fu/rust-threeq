use clap::ArgEnum;
use clap::Parser;

mod config;
mod entry_bench;
mod entry_verify;

#[derive(Parser, Debug, PartialEq)]
pub struct Args {
    #[clap(short = 'c', long = "config", long_help = "config file.")]
    config_file: String,

    #[clap(
        arg_enum,
        short = 't',
        long = "type",
        long_help = "type",
        default_value = "bench"
    )]
    sub_type: SubType,

    #[clap(long = "node", long_help = "this node id", default_value = " ")]
    node_id: String,
}

#[derive(ArgEnum, Debug, PartialEq, Clone)]
pub enum SubType {
    Bench,
    Verify,
}

pub async fn run(args: &Args) {
    match args.sub_type {
        SubType::Bench => entry_bench::run(args).await,
        SubType::Verify => entry_verify::run(args).await,
    }
}
