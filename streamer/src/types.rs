use alloy::primitives::{Address, FixedBytes, Uint, U256};
use serde::{Deserialize, Serialize};
use alloy::sol;

sol! {
    #[derive(Debug)]
    event OrderCreated(
        address indexed owner,
        uint24 indexed price_index,
        uint256 amount,
        uint40 order_id,
        bool is_buy,
        uint8 order_type,
        uint256 nonce
    );

    #[derive(Debug)]
    event OrderPosted(
        address indexed owner,
        uint24 indexed price_index,
        uint256 amount,
        bytes32 order_key,
        uint40 order_id,
        bool is_buy,
        bool is_head,
        uint256 nonce
    );

    #[derive(Debug)]
    event OrderRemoved(
        address indexed owner,
        uint24 indexed price_index,
        uint256 amount,
        bytes32 order_key,
        bool is_buy,
        bool is_head,
        uint256 nonce
    );

    #[derive(Debug)]
    event OrderFilled(
        address indexed taker_address,
        uint24 indexed price_index,
        uint256 amount,
        bytes32 maker_order_key,
        bytes32 taker_order_key,
        bool is_buy,
        uint256 maker_nonce,
        uint256 taker_nonce
    );

    #[derive(Debug)]
    struct OrderDetails {
        uint256 nonce;
        address owner;
        bytes32 order_key;
        uint256 price;
        uint256 quantity;
        uint40 order_id;
        bool is_head;
    }

    contract IOrderBook {
        uint256 public minPrice;
        uint256 public maxPrice;
        uint256 public minPriceIncrement;
        uint256 public constant PRICE_PRECISION = 1e6;

        function getOrderBookState(
            bool isBuy,
            uint256 limit
        ) public view returns(OrderDetails[] memory);
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum OrderType {
    BuyGTC,
    SellGTC,
    BuyIOC,
    SellIOC,
    BuyPostOnly,
    SellPostOnly,
}

impl From<u8> for OrderType {
    fn from(value: u8) -> Self {
        match value {
            1 => OrderType::BuyGTC,
            2 => OrderType::SellGTC,
            3 => OrderType::BuyIOC,
            4 => OrderType::SellIOC,
            5 => OrderType::BuyPostOnly,
            6 => OrderType::SellPostOnly,
            _ => panic!("Invalid order type: {}", value),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum OrderStatus {
    Open,
    Filled,
    Cancelled,
}

// Structure to represent an order in our in-memory state
#[derive(Debug, Clone, Serialize)]
pub struct OrderData {
    pub nonce: U256,
    pub orderbook: Address,
    pub owner: Address,
    pub quantity: U256,
    pub price_index: Uint<24, 1>,
    pub order_id: Uint<40, 1>,
    pub order_key: Option<FixedBytes<32>>,
    pub is_buy: bool,
    pub is_head: bool,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: FixedBytes<32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PostedOrder {
    pub nonce: U256,
    pub orderbook: Address,
    pub owner: Address,
    pub quantity: U256,
    pub price_index: Uint<24, 1>,
    pub order_id: Uint<40, 1>,
    pub order_key: FixedBytes<32>,
    pub is_buy: bool,
    pub is_head: bool,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: FixedBytes<32>,
}

#[derive(Debug, Clone, Serialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize)]
pub struct Trade {
    pub orderbook: Address,
    pub maker: Address,
    pub maker_order_key: FixedBytes<32>,
    pub taker: Address,
    pub taker_order_key: FixedBytes<32>,
    pub price_index: Uint<24, 1>,
    pub quantity: U256,
    pub taker_side: TradeSide,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: FixedBytes<32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderCancelled {
    pub orderbook: Address,
    pub owner: Address,
    pub quantity: U256,
    pub price_index: Uint<24, 1>,
    pub order_id: Uint<40, 1>,
    pub order_key: FixedBytes<32>,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: FixedBytes<32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PriceLevel {
    pub price: U256,
    pub quantity: U256,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderBook {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

#[derive(Debug, Clone, Serialize)]
pub enum OrderBookEvent {
    OrderPosted(PostedOrder),
    OrderCancelled(OrderCancelled),
    Match {
        trade: Trade,
        maker_order: OrderData,
        taker_order: OrderData,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketUpdate {
    pub market: Address,
    pub orderbook: OrderBook,
    pub event: Option<OrderBookEvent>,
    pub block_number: u64,
    pub block_timestamp: u64,
}

#[derive(Debug)]
pub enum ServerEvent {
    NewClient(tokio::net::TcpStream),
    DroppedClient(std::net::IpAddr),
    MarketUpdate(MarketUpdate),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMethod {
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientMessage {
    pub method: ClientMethod,
    pub params: Vec<Address>,
}

#[derive(Debug, Clone, Serialize)]
pub enum ServerMessage {
    MarketUpdate(MarketUpdate),
    Error { message: String },
}
