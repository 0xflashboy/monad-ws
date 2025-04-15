use alloy::{
    consensus::{SignableTransaction, TxLegacy},
    network::TxSignerSync,
    primitives::{Address, FixedBytes, TxKind, U256},
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    sol_types::SolCall,
};
use tracing::info;
use url::Url;

use crate::types::CountEmitter;

pub struct CountSender {
    contract_address: Address,
    http_url: Url,
    signer: PrivateKeySigner,
}

impl CountSender {
    pub fn new(contract_address: Address, http_url: Url) -> Self {
        let private_key = std::env::var("PRIVATE_KEY").unwrap();
        let private_bytes = hex::decode(&private_key[2..]).unwrap();
        let signer = PrivateKeySigner::from_bytes(&FixedBytes::from_slice(&private_bytes)).unwrap();

        Self {
            contract_address,
            http_url,
            signer,
        }
    }

    pub async fn run(&mut self) {
        let provider = ProviderBuilder::new().on_http(self.http_url.clone());

        let increment_call = CountEmitter::incrementCall {}.abi_encode();

        let sim_tx = alloy::rpc::types::TransactionRequest::default()
            .from(self.signer.address())
            .to(self.contract_address)
            .input(increment_call.clone().into());
        let gas_limit = provider.estimate_gas(sim_tx).await.unwrap() * 110 / 100;

        loop {
            let starting_nonce = provider
                .get_transaction_count(self.signer.address())
                .await
                .unwrap();

            info!(
                "Signing and sending 100 txs starting from nonce: {}",
                starting_nonce
            );

            let txs = (0..100)
                .map(|i| {
                    let mut tx = TxLegacy::default();
                    tx.nonce = starting_nonce + i;
                    tx.to = TxKind::Call(self.contract_address);
                    tx.value = U256::ZERO;
                    tx.input = increment_call.clone().into();
                    tx.chain_id = Some(10143);
                    tx.gas_price = 52_000_000_000u128;
                    tx.gas_limit = gas_limit;
                    let signature = self.signer.sign_transaction_sync(&mut tx).unwrap();
                    let signed_tx = tx.into_signed(signature);
                    let mut buf = Vec::new();
                    signed_tx.rlp_encode(&mut buf);
                    buf
                })
                .collect::<Vec<_>>();

            let mut hash = FixedBytes::default();
            for tx in txs { 
                let tx_hash = provider.send_raw_transaction(&tx).await.unwrap();
                hash = tx_hash.tx_hash().clone();
            }

            info!("Waiting for tx {} to confirm...", hash);
            loop {
                let receipt = provider.get_transaction_receipt(hash).await.unwrap();
                if receipt.is_some() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}
