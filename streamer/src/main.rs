use url::Url;

use serde::Deserialize;

use alloy::primitives::Address;
use tracing::{info, Level};

mod count_streamer;
use count_streamer::CountStreamer;

mod count_sender;
use count_sender::CountSender;

mod types;

#[derive(Deserialize)]
struct Config {
    http_url: String,
    ws_url: String,
    contract_address: Address,
}

fn load_config() -> Config {
    let config = std::fs::read_to_string("config.yml").unwrap();
    let config: Config = serde_yaml::from_str(&config).unwrap();
    config
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    let Config {
        http_url,
        ws_url,
        contract_address,
    } = load_config();

    let mut count_sender = CountSender::new(contract_address, Url::parse(&http_url).unwrap());

    let mut alloy_streamer = CountStreamer::new(contract_address, Url::parse(&ws_url).unwrap());
    let mut generic_streamer = CountStreamer::new(contract_address, Url::parse(&ws_url).unwrap());

    let results = tokio::join!(
        count_sender.run(),
        alloy_streamer.run_alloy(),
        generic_streamer.run_generic(),
    );

    info!("Results:");
    info!("count_sender: {:?}", results.0);
    info!("alloy_streamer: {:?}", results.1);
    info!("generic_streamer: {:?}", results.2);
}
