pragma solidity 0.8.11;
pragma abicoder v2;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@uniswap/v3-periphery/contracts/libraries/TransferHelper.sol";
import "@uniswap/v3-periphery/contracts/interfaces/ISwapRouter.sol";
import "./IWETH.sol";

contract Swaps {
  ISwapRouter public immutable swapRouter =
    ISwapRouter(0xE592427A0AEce92De3Edee1F18E0157C05861564);

  // This example swaps DAI/WETH9 for single path swaps and DAI/USDC/WETH9 for multi path swaps.

  // ISwapRouter public immutable swapRouter;
  // address public constant DAI = 0x6B175474E89094C44Da98b954EedeAC495271d0F;
  // address public constant WETH9 = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
  // uint24 public constant feeTier = 3000;

  constructor() {}

  function swapEthForWeth() public payable {
    IWETH weth = IWETH(0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2);
    weth.deposit{ value: msg.value }();
    weth.transfer(msg.sender, msg.value);
  }

  function swapExact(
    address tokenFrom,
    address tokenTo,
    uint24 feeTier,
    uint256 amountIn
  ) external returns (uint256 amountOut) {
    // Transfer the specified amount of WETH9 to this contract.
    TransferHelper.safeTransferFrom(tokenFrom, msg.sender, address(this), amountIn);

    // Approve the router to spend WETH9.
    TransferHelper.safeApprove(tokenFrom, address(swapRouter), amountIn);

    ISwapRouter.ExactInputSingleParams memory params = ISwapRouter
      .ExactInputSingleParams({
        tokenIn: tokenFrom,
        tokenOut: tokenTo,
        fee: feeTier,
        recipient: msg.sender,
        deadline: block.timestamp,
        amountIn: amountIn,
        amountOutMinimum: 0,
        sqrtPriceLimitX96: 0
      });
    // The call to `exactInputSingle` executes the swap.
    amountOut = swapRouter.exactInputSingle(params);
    return amountOut;
  }
}
