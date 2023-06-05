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

  constructor() {}

  function swapEthForWeth() external payable {
    IWETH weth = IWETH(0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2);
    weth.deposit{ value: msg.value }();
    weth.transfer(msg.sender, msg.value);
  }

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

  function borrow_token() external {
    IPoolAddressesProvider provider = IPoolAddressesProvider(
      0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e
    ); // mainnet address, https://docs.aave.com/developers/deployed-contracts/v3-mainnet/ethereum-mainnet

    IPool lendingPool = IPool(provider.getPool());

    // Input variables
    address daiAddress = 0x6B175474E89094C44Da98b954EedeAC495271d0F; // mainnet DAI
    address wethAddress = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2; // mainnet WETH

    uint256 amount = 5 * 1e18;
    uint16 referral = 0;
    address onBehalfOf = msg.sender;

    IERC20(wethAddress).transferFrom(msg.sender, address(this), amount);

    // Approve LendingPool contract to move your DAI
    IERC20(wethAddress).approve(address(lendingPool), amount);

    // Deposit 5 WETH
    lendingPool.deposit(wethAddress, amount, address(this), referral);

    lendingPool.setUserUseReserveAsCollateral(wethAddress, true);

    uint256 borrowAmount = 5 * 1e18;
    lendingPool.borrow(daiAddress, borrowAmount, 2, referral, address(this));

    IERC20(daiAddress).transferFrom(address(this), msg.sender, borrowAmount);
  }

  function repay_borrowed_token() external {
    IPoolAddressesProvider provider = IPoolAddressesProvider(
      0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e
    ); // mainnet address, https://docs.aave.com/developers/deployed-contracts/v3-mainnet/ethereum-mainnet

    IPool lendingPool = IPool(provider.getPool());

    // Input variables
    address daiAddress = 0x6B175474E89094C44Da98b954EedeAC495271d0F; // mainnet DAI
    address wethAddress = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2; // mainnet WETH

    uint256 borrowAmount = 5 * 1e18;
 
    IERC20(daiAddress).transferFrom(msg.sender, address(this), borrowAmount);

    // Approve LendingPool contract to move your DAI
    IERC20(daiAddress).approve(address(lendingPool), borrowAmount);

    lendingPool.repay(daiAddress, borrowAmount, 2, address(this));
    lendingPool.withdraw(wethAddress, borrowAmount, msg.sender);
  }
}
