// SPDX-License-Identifier: SEE LICENSE IN LICENSE
pragma solidity ^0.8.0;

import "forge-std/Script.sol";
import "../src/CountEmitter.sol";

contract DeployCountEmitter is Script {
    function run() external {
        // Start broadcasting transactions
        vm.startBroadcast();

        // Deploy the CountEmitter contract
        CountEmitter countEmitter = new CountEmitter();

        // Stop broadcasting transactions
        vm.stopBroadcast();

        // Log the deployed contract address
        console.log("CountEmitter deployed at:", address(countEmitter));
    }
}