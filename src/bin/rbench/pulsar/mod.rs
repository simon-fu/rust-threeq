use clap::ArgEnum;
use clap::Clap;

mod entry_bench;
mod entry_verify;

#[derive(Clap, Debug, PartialEq)]
pub struct Args {
    #[clap(short = 'c', long = "config", long_about = "config file.")]
    config_file: String,

    #[clap(
        arg_enum,
        short = 't',
        long = "type",
        long_about = "type",
        default_value = "bench"
    )]
    sub_type: SubType,
}

#[derive(ArgEnum, Debug, PartialEq)]
pub enum SubType {
    Bench,
    Verify,
}

pub async fn run(args: &Args) {
    match args.sub_type {
        SubType::Bench => entry_bench::run(&args.config_file).await,
        SubType::Verify => entry_verify::run(&args.config_file).await,
    }
}
