// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

contract CountEmitter {

    event CountAcquired(uint256 count);
    event CountCommitted(uint256 count);

    uint256 public count;

    constructor() {
        count = 1;
    }

    function increment() public {
        emit CountAcquired(count);
        emit CountCommitted(count);
        ++count;
    }
}
