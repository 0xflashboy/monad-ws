use alloy::sol;

sol! {
    contract CountEmitter {
        event CountAcquired(uint256 count);
        event CountCommitted(uint256 count);
        
        function increment() public;
    }
}