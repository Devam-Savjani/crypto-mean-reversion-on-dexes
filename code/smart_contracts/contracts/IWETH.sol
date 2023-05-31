pragma solidity ^0.8.11;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";

interface IWETH is IERC20 {
  function deposit() external payable;

  function withdraw(uint amount) external;

//   function approve(
//     address contractToApprove,
//     uint amount
//   ) external returns (bool);

//   function transferFrom(
//     address src,
//     address dst,
//     uint amount
//   ) external returns (bool);

//   function transfer(address dst, uint amount) external returns (bool);

//   mapping(address => uint) public balanceOf;

//   function balanceOf(address) external returns
}
