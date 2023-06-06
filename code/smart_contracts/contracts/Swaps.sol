pragma solidity 0.7.6;
pragma abicoder v2;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@uniswap/v3-periphery/contracts/libraries/TransferHelper.sol";
import "@uniswap/v3-periphery/contracts/interfaces/ISwapRouter.sol";
import "@uniswap/v3-core/contracts/interfaces/IUniswapV3Pool.sol";
import "@uniswap/v3-core/contracts/interfaces/callback/IUniswapV3SwapCallback.sol";
import "@uniswap/v3-core/contracts/libraries/TickMath.sol";
import "./IWETH.sol";
import "./IPoolAddressesProvider.sol";
import "./IPool.sol";
import "./DataTypes.sol";

contract Swaps is IUniswapV3SwapCallback {
  ISwapRouter public immutable swapRouter =
    ISwapRouter(0xE592427A0AEce92De3Edee1F18E0157C05861564);

  IPool public immutable lendingPool =
    IPool(
      IPoolAddressesProvider(0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e)
        .getPool()
    ); // mainnet address, https://docs.aave.com/developers/deployed-contracts/v3-mainnet/ethereum-mainnet

  address public immutable wethAddress =
    0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;

  constructor() {}

  function swapEthForWeth() external payable {
    IWETH weth = IWETH(wethAddress);
    weth.deposit{ value: msg.value }();
    weth.transfer(msg.sender, msg.value);
  }

  function swapWethForEth(uint256 amount) external payable {
    IWETH(wethAddress).transferFrom(msg.sender, address(this), amount);
    IWETH(wethAddress).withdraw(amount);
    msg.sender.transfer(address(this).balance);
  }

  receive() external payable {}

  function swapExactUsingRouter(
    address tokenFrom,
    address tokenTo,
    uint24 feeTier,
    uint256 amountIn
  ) external returns (uint256 amountOut) {
    // Transfer the specified amount of WETH9 to this contract.
    TransferHelper.safeTransferFrom(
      tokenFrom,
      msg.sender,
      address(this),
      amountIn
    );

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

  function swapExactUsingPool(
    address poolAddress,
    bool zeroForOne,
    int256 amountIn
  ) external returns (int256, int256) {
    IUniswapV3Pool pool = IUniswapV3Pool(poolAddress);

    return
      pool.swap(
        msg.sender,
        zeroForOne,
        amountIn,
        zeroForOne ? TickMath.MIN_SQRT_RATIO + 1 : TickMath.MAX_SQRT_RATIO - 1,
        abi.encode(poolAddress, pool.token0(), pool.token1(), msg.sender)
      );
  }

  function uniswapV3SwapCallback(
    int256 amount0Delta,
    int256 amount1Delta,
    bytes calldata data
  ) external override {
    (
      address poolAddress,
      address token0,
      address token1,
      address userAddress
    ) = abi.decode(data, (address, address, address, address));

    require(msg.sender == address(poolAddress));
    if (amount0Delta > 0) {
      IERC20(token0).transferFrom(
        userAddress,
        msg.sender,
        uint256(amount0Delta)
      );
    }
    if (amount1Delta > 0) {
      IERC20(token1).transferFrom(
        userAddress,
        msg.sender,
        uint256(amount1Delta)
      );
    }
  }

  function borrow_token(
    address tokenAddress,
    uint256 borrowAmount,
    uint256 collatoralAmount
  ) external {
    uint16 referral = 0;

    // Transfer 
    IERC20(wethAddress).transferFrom(
      msg.sender,
      address(this),
      collatoralAmount
    );

    // Approve LendingPool contract to move your WETH
    IERC20(wethAddress).approve(address(lendingPool), collatoralAmount);

    // Deposit collatoralAmount WETH
    lendingPool.deposit(wethAddress, collatoralAmount, address(this), referral);
    // Allow WETH to serve as Collateral
    lendingPool.setUserUseReserveAsCollateral(wethAddress, true);
    // Borrow token
    lendingPool.borrow(tokenAddress, borrowAmount, 2, referral, address(this));

    IERC20(tokenAddress).transferFrom(address(this), msg.sender, borrowAmount);
  }

  function repay_borrowed_token(address tokenAddress, uint256 repayAmount, uint256 collateralWithdrawAmount) external {

    IERC20(tokenAddress).transferFrom(msg.sender, address(this), repayAmount);

    // Approve LendingPool contract to move your DAI
    IERC20(tokenAddress).approve(address(lendingPool), repayAmount);

    lendingPool.repay(tokenAddress, repayAmount, 2, address(this));
    lendingPool.withdraw(wethAddress, collateralWithdrawAmount, msg.sender);
  }
}
