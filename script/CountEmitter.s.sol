// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Script} from "forge-std/Script.sol";
import {CountEmitter} from "../src/CountEmitter.sol";

contract CountEmitterScript is Script {
    CountEmitter public countEmitter;

    function run() public {
        uint256 deployerPrivateKey = vm.envUint("PRIVATE_KEY");
        vm.startBroadcast(deployerPrivateKey);

        countEmitter = new CountEmitter();

        vm.stopBroadcast();
    }
}
