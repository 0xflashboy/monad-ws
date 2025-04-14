use alloy::primitives::U256;
use alloy::primitives::{Address, FixedBytes, Uint};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
};
use alloy::providers::{Provider, ProviderBuilder, RootProvider, WsConnect};
use alloy::rpc::client::{ClientBuilder, RpcClient};
use alloy::rpc::types::{Filter, Log};
pub use alloy::sol;
use alloy::sol_types::{SolCall, SolEvent};
use anyhow::Result;
use futures::stream::{SplitSink, SplitStream};
use futures::{self, SinkExt};
use futures_util::StreamExt;
use hex;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info};
use url::Url;

mod types;
use types::*;

pub type DefaultProvider = FillProvider<
    JoinFill<
        alloy::providers::Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider,
>;

// Struct to track the orderbook state
#[derive(Debug, Clone)]
struct OrderBookState {
    address: Address,

    min_price: U256,
    max_price: U256,
    min_price_increment: U256,

    // Price level (index: u32) -> total quantity (U256)
    bids: BTreeMap<Uint<24, 1>, U256>,
    asks: BTreeMap<Uint<24, 1>, U256>,
    orders: HashMap<U256, OrderData>,
    highest_block: u64,
    witnessed_nonces: HashSet<U256>,
}

impl OrderBookState {
    fn new(address: Address, min_price: U256, max_price: U256, min_price_increment: U256) -> Self {
        Self {
            address,
            min_price,
            max_price,
            min_price_increment,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
            highest_block: 0,
            witnessed_nonces: HashSet::new(),
        }
    }

    fn price_to_index(&self, price: U256) -> Uint<24, 1> {
        if price < self.min_price + self.min_price_increment || price > self.max_price {
            panic!(
                "Price out of range: {} - Min: {} - Max: {}",
                price, self.min_price, self.max_price
            );
        }
        let index = (price - self.min_price) / self.min_price_increment;
        Uint::<24, 1>::from(index)
    }

    fn index_to_price(&self, index: Uint<24, 1>) -> U256 {
        if index > Uint::from(9_999_999) || index == Uint::ZERO {
            panic!("Index out of range: {}", index);
        }
        self.min_price + (U256::from(index) * self.min_price_increment)
    }

    fn add_existing_order(
        &mut self,
        order_details: OrderDetails,
        is_buy: bool,
        block_number: u64,
        block_timestamp: u64,
    ) {
        if order_details.price == Uint::ZERO {
            return;
        }
        debug!("Adding existing order: {:?}", order_details);
        self.witnessed_nonces.insert(order_details.nonce);
        let price_index = self.price_to_index(order_details.price);
        let order = OrderData {
            nonce: order_details.nonce,
            orderbook: self.address,
            owner: order_details.owner,
            quantity: order_details.quantity,
            price_index,
            order_id: order_details.order_id,
            order_key: Some(order_details.order_key),
            is_buy,
            is_head: order_details.is_head,
            order_type: if is_buy {
                OrderType::BuyGTC
            } else {
                OrderType::SellGTC
            },
            status: OrderStatus::Open,
            block_number,
            block_timestamp,
            transaction_hash: FixedBytes::ZERO,
        };
        self.orders.insert(order.nonce, order);

        let side = if is_buy {
            &mut self.bids
        } else {
            &mut self.asks
        };
        let current_qty = side.entry(price_index).or_insert(U256::ZERO);
        *current_qty += order_details.quantity;
    }

    // Apply an OrderCreated event to the orderbook
    fn apply_order_created(
        &mut self,
        order_created: OrderCreated,
        block_number: u64,
        block_timestamp: u64,
        transaction_hash: FixedBytes<32>,
    ) {
        self.witnessed_nonces.insert(order_created.nonce);
        debug!(
            "Applying OrderCreated event tx hash {}: {:?}",
            transaction_hash, order_created
        );
        let order = OrderData {
            nonce: order_created.nonce.clone(),
            orderbook: self.address,
            owner: order_created.owner,
            quantity: order_created.amount,
            price_index: order_created.price_index,
            order_id: order_created.order_id,
            order_key: None,
            is_buy: order_created.is_buy,
            is_head: false,
            order_type: OrderType::from(order_created.order_type),
            status: OrderStatus::Open,
            block_number,
            block_timestamp,
            transaction_hash,
        };

        self.orders.insert(order_created.nonce, order);
    }

    // Apply an OrderPosted event to the orderbook
    fn apply_order_posted(
        &mut self,
        order_posted: OrderPosted,
        block_number: u64,
        block_timestamp: u64,
        transaction_hash: FixedBytes<32>,
    ) -> MarketUpdate {
        debug!(
            "Applying OrderPosted event block height {}: {:?}",
            transaction_hash, order_posted
        );
        let maybe_order = self.orders.get_mut(&order_posted.nonce);
        if maybe_order.is_none() {
            panic!(
                "Order nonce {} not found; witnessed: {}",
                order_posted.nonce,
                self.witnessed_nonces.contains(&order_posted.nonce)
            );
        }
        let order = maybe_order.unwrap();
        order.order_key = Some(order_posted.order_key.clone());
        order.quantity = order_posted.amount;

        // Update price level quantity
        let price_levels = if order_posted.is_buy {
            &mut self.bids
        } else {
            &mut self.asks
        };

        // Directly add the U256 amount
        let current_qty = price_levels
            .entry(order_posted.price_index)
            .or_insert(U256::ZERO);
        *current_qty = *current_qty + order_posted.amount;

        let posted_order = PostedOrder {
            nonce: order_posted.nonce,
            orderbook: self.address,
            owner: order.owner,
            quantity: order_posted.amount,
            price_index: order_posted.price_index,
            order_id: order_posted.order_id,
            order_key: order_posted.order_key,
            is_buy: order_posted.is_buy,
            is_head: order_posted.is_head,
            order_type: if order_posted.is_buy {
                OrderType::BuyGTC
            } else {
                OrderType::SellGTC
            },
            status: OrderStatus::Open,
            block_number,
            block_timestamp,
            transaction_hash,
        };

        self.create_market_update(
            Some(OrderBookEvent::OrderPosted(posted_order)),
            block_number,
            block_timestamp,
        )
    }

    // Apply an OrderFilled event to the orderbook
    fn apply_order_filled(
        &mut self,
        order_filled: OrderFilled,
        block_number: u64,
        block_timestamp: u64,
        transaction_hash: FixedBytes<32>,
    ) -> MarketUpdate {
        debug!(
            "Applying OrderFilled event block height {}: {:?}",
            transaction_hash, order_filled
        );
        // Update the price level quantity
        let price_levels = if order_filled.is_buy {
            &mut self.asks
        } else {
            &mut self.bids
        };

        let current_qty = price_levels.get_mut(&order_filled.price_index).unwrap();
        *current_qty -= order_filled.amount;
        // If quantity is now zero, remove the price level
        if *current_qty == U256::ZERO {
            // Compare with U256::ZERO
            price_levels.remove(&order_filled.price_index);
        }

        // Find the maker address from our orders map
        let maybe_maker_order = self.orders.remove(&order_filled.maker_nonce);
        if maybe_maker_order.is_none() {
            panic!(
                "Maker order nonce {} not found; witnessed: {}",
                order_filled.maker_nonce,
                self.witnessed_nonces.contains(&order_filled.maker_nonce)
            );
        }
        let mut maker_order = maybe_maker_order.unwrap();
        maker_order.quantity -= order_filled.amount;
        if maker_order.quantity == U256::ZERO {
            maker_order.status = OrderStatus::Filled;
        } else {
            self.orders.insert(maker_order.nonce, maker_order.clone());
        }

        let maybe_taker_order = self.orders.remove(&order_filled.taker_nonce);
        if maybe_taker_order.is_none() {
            panic!(
                "Taker order nonce {} not found; witnessed: {}",
                order_filled.taker_nonce,
                self.witnessed_nonces.contains(&order_filled.taker_nonce)
            );
        }
        let mut taker_order = maybe_taker_order.unwrap();
        taker_order.quantity -= order_filled.amount;
        if taker_order.quantity == U256::ZERO {
            taker_order.status = OrderStatus::Filled;
        } else {
            self.orders.insert(taker_order.nonce, taker_order.clone());
        }

        let trade = Trade {
            orderbook: self.address,
            maker: maker_order.owner,
            maker_order_key: order_filled.maker_order_key,
            taker: order_filled.taker_address,
            taker_order_key: order_filled.taker_order_key,
            price_index: order_filled.price_index,
            quantity: order_filled.amount,
            taker_side: if order_filled.is_buy {
                TradeSide::Buy
            } else {
                TradeSide::Sell
            },
            block_number,
            block_timestamp,
            transaction_hash,
        };

        self.create_market_update(
            Some(OrderBookEvent::Match {
                trade,
                maker_order,
                taker_order,
            }),
            block_number,
            block_timestamp,
        )
    }

    // Apply an OrderRemoved event to the orderbook
    fn apply_order_removed(
        &mut self,
        order_removed: OrderRemoved,
        block_number: u64,
        block_timestamp: u64,
        transaction_hash: FixedBytes<32>,
    ) -> MarketUpdate {
        debug!(
            "Applying OrderRemoved event block height {}: {:?}",
            transaction_hash, order_removed
        );
        // Update the price level quantity
        let price_levels = if order_removed.is_buy {
            &mut self.bids
        } else {
            &mut self.asks
        };
        info!("Order removed @ price {}", order_removed.price_index);
        if let Some(current_qty) = price_levels.get_mut(&order_removed.price_index) {
            *current_qty -= order_removed.amount;

            // If quantity is now zero, remove the price level
            if *current_qty == U256::ZERO {
                price_levels.remove(&order_removed.price_index);
            }
        }

        // Remove the order from our tracker
        let maybe_order = self.orders.remove(&order_removed.nonce);
        if maybe_order.is_none() {
            panic!(
                "Order removed nonce {} not found; witnessed: {}",
                order_removed.nonce,
                self.witnessed_nonces.contains(&order_removed.nonce)
            );
        }
        let order = maybe_order.unwrap();
        let order_cancelled = OrderCancelled {
            order_key: order_removed.order_key,
            orderbook: self.address,
            owner: order.owner,
            quantity: order.quantity,
            price_index: order_removed.price_index,
            order_id: order.order_id,
            order_type: order.order_type,
            status: OrderStatus::Cancelled,
            block_number,
            block_timestamp,
            transaction_hash,
        };

        self.create_market_update(
            Some(OrderBookEvent::OrderCancelled(order_cancelled)),
            block_number,
            block_timestamp,
        )
    }

    // Create a MarketUpdate from the current state
    fn create_market_update(
        &self,
        event: Option<OrderBookEvent>,
        block_number: u64,
        block_timestamp: u64,
    ) -> MarketUpdate {
        // Convert price levels to PriceLevel structs
        let bids = self
            .bids
            .iter()
            .map(|(price_index, quantity)| {
                // Key is u32 price_index
                PriceLevel {
                    price: self.index_to_price(*price_index), // Convert index back to price (assuming simple mapping)
                    quantity: *quantity,                      // Quantity is already U256
                }
            })
            .collect::<Vec<_>>();

        let asks = self
            .asks
            .iter()
            .map(|(price_index, quantity)| {
                // Key is u32 price_index
                PriceLevel {
                    price: self.index_to_price(*price_index), // Convert index back to price
                    quantity: *quantity,                      // Quantity is already U256
                }
            })
            .collect::<Vec<_>>();

        MarketUpdate {
            market: self.address,
            orderbook: OrderBook { bids, asks },
            event,
            block_number,
            block_timestamp,
        }
    }

    fn dump(&self, label: &str, on_panic: bool) {
        debug!("{}: Bids: {:#?}", label, self.bids);
        debug!("{}: Asks: {:#?}", label, self.asks);
        if on_panic {
            info!("{}: Bids: {:#?}", label, self.bids);
            info!("{}: Asks: {:#?}", label, self.asks);
        }
    }
}

// Helper function to parse the ABI-encoded response from getOrderBookState
fn parse_orderbook_state_response(hex_data: String) -> Result<Vec<OrderDetails>> {
    let hex_data = hex::decode(&hex_data[2..]).unwrap();
    let out = IOrderBook::getOrderBookStateCall::abi_decode_returns(&hex_data, true)?;
    Ok(out._0)
}

// Process logs for a specific orderbook and return market updates
fn process_log(log: &Log, orderbook_state: &mut OrderBookState) -> Result<Option<MarketUpdate>> {
    let block_number = log.block_number.unwrap();
    if block_number < orderbook_state.highest_block {
        // Allow logs at highest block since multiple logs can be emitted per block
        return Ok(None);
    }
    orderbook_state.highest_block = u64::max(orderbook_state.highest_block, block_number);
    let block_timestamp = log.block_timestamp.unwrap();
    let topics = log.topics();
    if topics.len() < 1 {
        return Err(anyhow::anyhow!("Log missing indexed topics: {:?}", log));
    }
    let tx_hash: FixedBytes<32> = log.transaction_hash.unwrap();

    let market_update = match topics[0] {
        OrderCreated::SIGNATURE_HASH => {
            let order_created: OrderCreated = log.log_decode()?.inner.data;
            orderbook_state.apply_order_created(
                order_created,
                block_number,
                block_timestamp,
                tx_hash,
            );
            None
        }
        OrderPosted::SIGNATURE_HASH => {
            let order_posted: OrderPosted = log.log_decode()?.inner.data;
            Some(orderbook_state.apply_order_posted(
                order_posted,
                block_number,
                block_timestamp,
                tx_hash,
            ))
        }
        OrderFilled::SIGNATURE_HASH => {
            let order_filled: OrderFilled = log.log_decode()?.inner.data;
            Some(orderbook_state.apply_order_filled(
                order_filled,
                block_number,
                block_timestamp,
                tx_hash,
            ))
        }
        OrderRemoved::SIGNATURE_HASH => {
            let order_removed: OrderRemoved = log.log_decode()?.inner.data;
            Some(orderbook_state.apply_order_removed(
                order_removed,
                block_number,
                block_timestamp,
                tx_hash,
            ))
        }
        _ => panic!("Unknown event: {:?}", log),
    };

    Ok(market_update)
}

async fn get_existing_orders(
    orderbook_address: Address,
    block_height: u64,
    http_client: &RpcClient,
) -> Result<(Vec<OrderDetails>, Vec<OrderDetails>)> {
    let block_param = format!("0x{:x}", block_height);
    let mut buy_orders = Vec::new();
    let mut sell_orders = Vec::new();
    for is_buy in [true, false] {
        let calldata = IOrderBook::getOrderBookStateCall {
            isBuy: is_buy,
            limit: U256::from(15_000),
        };
        let call_params = json!({
            "to": orderbook_address.to_string(),
            "data": format!("0x{}", hex::encode(calldata.abi_encode()))
        });
        let hex_data = http_client
            .request::<_, String>("eth_call", [call_params, block_param.clone().into()])
            .await?;

        let side_order_details = parse_orderbook_state_response(hex_data).unwrap();
        if is_buy {
            buy_orders = side_order_details;
        } else {
            sell_orders = side_order_details;
        }
    }

    Ok((buy_orders, sell_orders))
}

async fn get_orderbook_config(
    orderbook_address: Address,
    block_param: &String,
    http_client: &RpcClient,
) -> Result<(U256, U256, U256)> {
    let min_price = {
        let call = IOrderBook::minPriceCall {};
        let call_params = json!({
            "to": orderbook_address.to_string(),
            "data": format!("0x{}", hex::encode(call.abi_encode()))
        });
        http_client
            .request::<_, U256>("eth_call", [call_params, block_param.clone().into()])
            .await?
    };

    let max_price = {
        let call = IOrderBook::maxPriceCall {};
        let call_params = json!({
            "to": orderbook_address.to_string(),
            "data": format!("0x{}", hex::encode(call.abi_encode()))
        });
        http_client
            .request::<_, U256>("eth_call", [call_params, block_param.clone().into()])
            .await?
    };

    let min_price_increment = {
        let call = IOrderBook::minPriceIncrementCall {};
        let call_params = json!({
            "to": orderbook_address.to_string(),
            "data": format!("0x{}", hex::encode(call.abi_encode()))
        });
        http_client
            .request::<_, U256>("eth_call", [call_params, block_param.clone().into()])
            .await?
    };

    Ok((min_price, max_price, min_price_increment))
}

async fn get_fresh_orderbook_state(
    orderbook_address: Address,
    block_height: u64,
    http_client: &RpcClient,
    http_provider: &DefaultProvider,
) -> Result<(OrderBookState, u64)> {
    let block_timestamp = http_provider
        .get_block_by_number(alloy::eips::BlockNumberOrTag::Number(block_height))
        .await?
        .unwrap()
        .header
        .timestamp;
    let block_param = format!("0x{:x}", block_height);
    let (min_price, max_price, min_price_increment) =
        get_orderbook_config(orderbook_address, &block_param, http_client).await?;
    let mut orderbook_state =
        OrderBookState::new(orderbook_address, min_price, max_price, min_price_increment);
    orderbook_state.highest_block = block_height;
    Ok((orderbook_state, block_timestamp))
}

fn insert_existing_orders(
    buy_orders: Vec<OrderDetails>,
    sell_orders: Vec<OrderDetails>,
    orderbook_state: &mut OrderBookState,
    block_number: u64,
    block_timestamp: u64,
) {
    for (is_buy, order_set) in [(true, buy_orders), (false, sell_orders)] {
        for order in order_set {
            orderbook_state.add_existing_order(order, is_buy, block_number, block_timestamp);
        }
    }
}

fn check_book_consistency(book_snapshot: &OrderBookState, recovered_book: &OrderBookState) {
    if book_snapshot.bids.len() != recovered_book.bids.len() {
        book_snapshot.dump("Current", true);
        recovered_book.dump("Snapshot", true);
        panic!(
            "Bids length mismatch: {} (current) != {} (actual)",
            book_snapshot.bids.len(),
            recovered_book.bids.len()
        );
    }
    if book_snapshot.asks.len() != recovered_book.asks.len() {
        book_snapshot.dump("Current", true);
        recovered_book.dump("Snapshot", true);
        panic!("Asks length mismatch: {} (current) != {} (actual)\nDump:\nCurrent: {:#?}\nActual: {:#?}", book_snapshot.asks.len(), recovered_book.asks.len(), book_snapshot.asks, recovered_book.asks);
    }
    for (price, quantity) in recovered_book.bids.iter() {
        let current_quantity = book_snapshot.bids.get(price).unwrap_or(&U256::ZERO);
        if current_quantity != quantity {
            book_snapshot.dump("Current", true);
            recovered_book.dump("Snapshot", true);
            panic!("Bids quantity mismatch @ price {}: {} (current) != {} (actual)\nDump:\nCurrent: {:#?}\nActual: {:#?}", price, current_quantity, quantity, book_snapshot.bids, recovered_book.bids);
        }
    }
    for (price, quantity) in recovered_book.asks.iter() {
        let current_quantity = book_snapshot.asks.get(price).unwrap_or(&U256::ZERO);
        if current_quantity != quantity {
            book_snapshot.dump("Current", true);
            recovered_book.dump("Snapshot", true);
            panic!("Asks quantity mismatch @ price {}: {} (current) != {} (actual)\nDump:\nCurrent: {:#?}\nActual: {:#?}", price, current_quantity, quantity, book_snapshot.asks, recovered_book.asks);
        }
    }
}

pub struct BookStreamer {
    http_client: RpcClient,
    http_provider: DefaultProvider,
    ws_url: Url,
    orderbook_addresses: Vec<Address>,
    event_hashes: Vec<FixedBytes<32>>,
    orderbook_states: HashMap<Address, OrderBookState>,
}

impl BookStreamer {
    pub fn new(orderbook_addresses: Vec<Address>, http_url: Url, ws_url: Url) -> Self {

        let event_hashes = vec![
            OrderCreated::SIGNATURE_HASH,
            OrderFilled::SIGNATURE_HASH,
            OrderPosted::SIGNATURE_HASH,
            OrderRemoved::SIGNATURE_HASH,
        ];

        Self {
            http_client: ClientBuilder::default().http(http_url.clone()),
            http_provider: ProviderBuilder::new().on_http(http_url),
            ws_url,
            orderbook_addresses,
            event_hashes,
            orderbook_states: HashMap::new(),
        }
    }

    async fn get_existing_orders(
        &self,
        orderbook_address: Address,
        block_height: u64,
    ) -> Result<(Vec<OrderDetails>, Vec<OrderDetails>)> {
        return get_existing_orders(orderbook_address, block_height, &self.http_client).await;
    }

    async fn get_fresh_orderbook_state(
        &self,
        orderbook_address: Address,
        block_height: u64,
    ) -> Result<(OrderBookState, u64)> {
        return get_fresh_orderbook_state(
            orderbook_address,
            block_height,
            &self.http_client,
            &self.http_provider,
        )
        .await;
    }

    async fn construct_orderbook_state_at_block(
        &self,
        orderbook_address: Address,
        block_height: u64,
    ) -> Result<OrderBookState> {
        let (mut orderbook_state, block_timestamp) = self
            .get_fresh_orderbook_state(orderbook_address, block_height)
            .await?;

        let (buy_orders, sell_orders) = self
            .get_existing_orders(orderbook_address, block_height)
            .await?;
        insert_existing_orders(
            buy_orders,
            sell_orders,
            &mut orderbook_state,
            block_height,
            block_timestamp,
        );

        Ok(orderbook_state)
    }

    fn send_market_update(
        &self,
        market_update: MarketUpdate,
        event_sender: &mpsc::UnboundedSender<ServerEvent>,
    ) {
        let _ = event_sender
            .send(ServerEvent::MarketUpdate(market_update))
            .unwrap();
    }

    async fn initialize_orderbooks(
        &mut self,
        event_sender: &mpsc::UnboundedSender<ServerEvent>,
    ) -> Result<()> {
        let initial_block_number = self.http_provider.get_block_number().await?;
        let initial_block_timestamp = self
            .http_provider
            .get_block_by_number(alloy::eips::BlockNumberOrTag::Number(initial_block_number))
            .await?
            .unwrap()
            .header
            .timestamp;

        info!("Initial block height: {}", initial_block_number);

        self.orderbook_states.clear();
        let mut tasks = Vec::new();
        for &orderbook_address in &self.orderbook_addresses {
            tasks.push(
                self.construct_orderbook_state_at_block(orderbook_address, initial_block_number),
            );
        }
        let orderbook_states = futures::future::join_all(tasks).await;
        for (orderbook_address, orderbook_state) in
            self.orderbook_addresses.iter().zip(orderbook_states)
        {
            let mut orderbook_state = orderbook_state?;
            orderbook_state.highest_block = initial_block_number + 1; // +1 to avoid re-processing the initial block
            let initial_update = orderbook_state.create_market_update(
                None,
                initial_block_number,
                initial_block_timestamp,
            );

            orderbook_state.dump("Initial", false);
            self.send_market_update(initial_update, event_sender);

            self.orderbook_states
                .insert(orderbook_address.clone(), orderbook_state);
        }

        Ok(())
    }

    fn spawn_reconciliation_task(
        &self,
        rpc_client: RpcClient,
        http_provider: DefaultProvider,
    ) -> mpsc::Sender<OrderBookState> {
        let (sender, mut receiver) = mpsc::channel(128);
        tokio::spawn(async move {
            loop {
                let book_snapshot: OrderBookState = receiver.recv().await.unwrap();
                let block_height = book_snapshot.highest_block;
                let maybe_recovered_book = get_fresh_orderbook_state(
                    book_snapshot.address,
                    block_height,
                    &rpc_client,
                    &http_provider,
                )
                .await;
                if let Err(e) = maybe_recovered_book {
                    error!(
                        "Error fetching recovered book for {} at block {}: {}",
                        book_snapshot.address, block_height, e
                    );
                    continue;
                }
                let (mut recovered_book, block_timestamp) = maybe_recovered_book.unwrap();

                let maybe_existing_orders =
                    get_existing_orders(recovered_book.address, block_height, &rpc_client).await;
                if let Err(e) = maybe_existing_orders {
                    error!(
                        "Error fetching existing orders for {} at block {}: {}",
                        recovered_book.address, block_height, e
                    );
                    continue;
                }
                let (buy_orders, sell_orders) = maybe_existing_orders.unwrap();
                insert_existing_orders(
                    buy_orders,
                    sell_orders,
                    &mut recovered_book,
                    block_height,
                    block_timestamp,
                );

                check_book_consistency(&book_snapshot, &recovered_book);
                info!(
                    "Orderbook state reconciliation successful for {} at block {}",
                    book_snapshot.address, block_height
                );
            }
        });
        sender
    }

    async fn run_subscription(
        &mut self,
        mut read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        mut write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        event_sender: &mpsc::UnboundedSender<ServerEvent>,
    ) -> Result<()> {
        let mut reconcile_interval = tokio::time::interval(Duration::from_secs(10));
        let mut book_to_last_finalized_snapshot = HashMap::new();

        let reconcile_sender =
            self.spawn_reconciliation_task(self.http_client.clone(), self.http_provider.clone());

        loop {
            tokio::select! {
                maybe_msg = read.next() => {
                    if maybe_msg.is_none() {
                        error!("WebSocket stream closed");
                        break;
                    }
                    let msg_result = maybe_msg.unwrap();
                    match msg_result {
                        Ok(message) => {
                            match message {
                                Message::Text(text) => {
                                    let json: Value = serde_json::from_str(&text)?;
                                    if json["params"]["result"].is_object() {
                                        let log: Log =
                                            serde_json::from_value(json["params"]["result"].clone())?;
                                        debug!("Received log: {:?}", log);

                                        let address = log.address();
                                        let book = self.orderbook_states.get_mut(&address).unwrap();
                                        let tx_hash = log.transaction_hash.unwrap();
                                        if log.block_number.unwrap() > book.highest_block {
                                            book_to_last_finalized_snapshot.insert(address, book.clone());
                                        }

                                        let maybe_update = process_log(&log, book).unwrap();
                                        if let Some(update) = maybe_update {
                                            book.dump("Applied update", false);
                                            info!("Sending update - tx hash {}", tx_hash);
                                            self.send_market_update(update, event_sender);
                                        }
                                    } else if json["result"].is_string() {
                                        info!("Subscription ID: {:?}", json["result"].to_string());
                                    } else {
                                        error!("Unexpected message format: {:?}", json);
                                        break;
                                    }
                                }
                                Message::Binary(_) => {
                                    error!("Unexpected binary message");
                                    break;
                                }
                                Message::Ping(data) => {
                                    write.send(Message::Pong(data)).await?;
                                }
                                Message::Pong(_) => {
                                    // Ignore pongs
                                }
                                Message::Close(_) => {
                                    error!("WebSocket connection received close message");
                                    // This will break the loop in the next iteration
                                }
                                _ => (),
                            }
                        }
                        Err(e) => {
                            error!("WebSocket error: {}", e);
                            break;
                        }
                    }
                }
                _ = reconcile_interval.tick() => {
                    for book_snapshot in book_to_last_finalized_snapshot.values() {
                        reconcile_sender.send(book_snapshot.clone()).await.unwrap();
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn run_generic(&mut self, event_sender: mpsc::UnboundedSender<ServerEvent>) -> Result<()> {
        let ws = connect_async(self.ws_url.clone()).await?.0;
        let (mut write, read) = ws.split();

        info!("Subscribing to WebSocket logs");
        let sub_filter: Filter = Filter::new()
            .address(self.orderbook_addresses.clone())
            .event_signature(self.event_hashes.clone());

        let msg = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["logs", sub_filter],
            "id": 1
        });
        write.send(Message::Text(msg.to_string())).await?;

        info!("Initializing orderbooks");
        self.initialize_orderbooks(&event_sender).await?;

        info!("Listening for logs");
        self.run_subscription(read, write, &event_sender).await?;

        Ok(())
    }

    pub async fn run_alloy(&mut self) -> Result<()> {
        let ws = WsConnect::new(self.ws_url.clone());
        let provider = ProviderBuilder::new().on_ws(ws).await?;
        let filter = Filter::new()
            .address(self.orderbook_addresses.clone())
            .event_signature(self.event_hashes.clone());
        let sub = provider.subscribe_logs(&filter).await?;
        let mut stream = sub.into_stream();
        let mut created_order_nonces = HashSet::new();
        loop {
            let maybe_log = stream.next().await;
            if maybe_log.is_none() {
                error!("Subscription stream closed");
                break;
            }
            let log = maybe_log.unwrap();
            let address = log.address();
            match log.topic0() {
                Some(&OrderCreated::SIGNATURE_HASH) => {
                    let order_created = OrderCreated::decode(log.data()).unwrap();
                    assert!(created_order_nonces.insert(order_created.nonce));
                    println!("Order created: {:?}", order_created);
                },
                Some(&OrderPosted::SIGNATURE_HASH) => {
                    let order_posted = OrderPosted::decode(log.data()).unwrap();
                    assert!(created_order_nonces.contains(&order_posted.nonce));
                    println!("Order posted: {:?}", order_posted);
                },
                Some(&OrderFilled::SIGNATURE_HASH) => {
                    let order_filled = OrderFilled::decode(log.data()).unwrap();
                    assert!(created_order_nonces.contains(&order_posted.nonce));
                    println!("Order filled: {:?}", order_filled);
                },
                Some(&)
                _ => (),
            }
            if log.block_number.unwrap() > book.highest_block {
                book_to_last_finalized_snapshot.insert(address, book.clone());
            }
        }
        Ok(())
    }
}

pub async fn run_streamer(orderbook_addresses: Vec<Address>, http_url: Url, ws_url: Url, event_sender: mpsc::UnboundedSender<ServerEvent>) {
    let mut streamer = BookStreamer::new(orderbook_addresses, http_url, ws_url);
    loop {
        if let Err(e) = streamer.run(event_sender.clone()).await {
            error!("Streamer error: {}", e);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

fn main() {
    let orderbook_addresses = vec![Address::from_str("0x0000000000000000000000000000000000000000").unwrap()];
    let http_url = Url::parse("https://mainnet.infura.io/v3/YOUR_INFURA_API_KEY").unwrap();
    let ws_url = Url::parse("wss://mainnet.infura.io/ws/v3/YOUR_INFURA_API_KEY").unwrap();
}