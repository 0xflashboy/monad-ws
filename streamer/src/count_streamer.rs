use alloy::primitives::U256;
use alloy::primitives::{Address, FixedBytes};
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::Result;
use futures::{self, SinkExt};
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info};
use url::Url;

use crate::types::CountEmitter::{CountAcquired, CountCommitted};

pub struct CountStreamer {
    contract_address: Address,
    ws_url: Url,
    highest_count: U256,
    event_hashes: Vec<FixedBytes<32>>,
}

impl CountStreamer {
    pub fn new(contract_address: Address, ws_url: Url) -> Self {
        Self {
            contract_address,
            ws_url,
            highest_count: U256::ZERO,
            event_hashes: vec![CountAcquired::SIGNATURE_HASH, CountCommitted::SIGNATURE_HASH],
        }
    }

    fn process_log(&mut self, log: &Log, is_alloy: bool) {
        match log.topic0() {
            Some(&CountAcquired::SIGNATURE_HASH) => {
                let event =
                    CountAcquired::abi_decode_data(log.data().data.to_vec().as_slice(), true).unwrap();
                info!("Received CountAcquired event, count {:?}", event.0);
                if self.highest_count != U256::ZERO {
                    assert!(
                        self.highest_count == event.0.checked_sub(U256::ONE).unwrap(),
                        "Did not witness new count {} in CountAcquired event, (is_alloy: {})",
                        event.0,
                        is_alloy
                    );
                }
                self.highest_count = event.0;
            }
            Some(&CountCommitted::SIGNATURE_HASH) => {
                let event =
                    CountCommitted::abi_decode_data(log.data().data.to_vec().as_slice(), true).unwrap();
                info!("Received CountCommitted event, count {:?}", event.0);
                assert!(
                    self.highest_count == event.0,
                    "Did not witness new count {} in CountAcquired event, (is_alloy: {})",
                    event.0,
                    is_alloy
                );
            }
            _ => panic!(),
        }
    }

    pub async fn run_generic(&mut self) -> Result<()> {
        let ws = connect_async(self.ws_url.clone()).await?.0;
        let (mut write, mut read) = ws.split();

        info!("Subscribing to WebSocket logs");
        let sub_filter: Filter = Filter::new()
            .address(self.contract_address.clone())
            .event_signature(self.event_hashes.clone());

        let msg = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["logs", sub_filter],
            "id": 1
        });
        write.send(Message::Text(msg.to_string())).await?;

        info!("Listening for logs");
        loop {
            let maybe_msg = read.next().await;
            if maybe_msg.is_none() {
                return Err(anyhow::anyhow!("WebSocket stream closed"));
            }
            let msg_result = maybe_msg.unwrap();
            if msg_result.is_err() {
                return Err(anyhow::anyhow!(
                    "WebSocket error: {}",
                    msg_result.err().unwrap()
                ));
            }
            let msg = msg_result.unwrap();
            if let Message::Text(text) = msg {
                let json: Value = serde_json::from_str(&text)?;
                if json["params"]["result"].is_object() {
                    let log: Log = serde_json::from_value(json["params"]["result"].clone())?;
                    debug!("Received log: {:?}", log);
                    self.process_log(&log, false);
                } else if json["result"].is_string() {
                    info!("Subscription ID: {:?}", json["result"].to_string());
                } else {
                    error!("Unexpected message format: {:?}", json);
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn run_alloy(&mut self) -> Result<()> {
        let ws = WsConnect::new(self.ws_url.clone());
        let provider = ProviderBuilder::new().on_ws(ws).await?;
        let filter = Filter::new()
            .address(self.contract_address.clone())
            .event_signature(self.event_hashes.clone());
        let mut sub = provider.subscribe_logs(&filter).await?;
        loop {
            let maybe_log = sub.recv().await;
            if maybe_log.is_err() {
                error!("Subscription stream error: {:?}", maybe_log.err().unwrap());
                break;
            }
            let log = maybe_log.unwrap();
            self.process_log(&log, true);
        }
        Ok(())
    }
}
