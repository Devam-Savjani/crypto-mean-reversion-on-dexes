pragma solidity 0.8.11;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "./IWETH.sol";

contract Swaps {
  IWETH weth;

  constructor(address _weth) {
    weth = IWETH(_weth);
  }

  function swapEthForWeth() public payable {
    weth.deposit{value: msg.value}();
    weth.transfer(msg.sender, msg.value);
  }
}