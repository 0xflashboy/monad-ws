// SPDX-License-Identifier: SEE LICENSE IN LICENSE
pragma solidity ^0.8.0;

contract CountEmitter {
    
    event CountWitnessed(uint256 count);
    event CountCommitted(uint256 count);

    uint256 public nonce;

    constructor() {
        nonce = 0;
    }

    function emitCount() external {
        emit CountWitnessed(nonce);
        emit CountCommitted(nonce);
        nonce++;
    }
}
