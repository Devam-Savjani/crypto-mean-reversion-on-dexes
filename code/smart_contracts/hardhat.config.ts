import "@nomiclabs/hardhat-ethers";
import "@nomiclabs/hardhat-waffle";
import "@typechain/hardhat";
import "solidity-coverage";

export default {
  solidity: {
    version: "0.7.6",
    settings: {
      optimizer: {
        enabled: true,
        runs: 200,
      },
    },
  },
  networks: {
    hardhat: {
      forking: {
        url: "https://eth-mainnet.g.alchemy.com/v2/F5gSnV_OJ77nOQWI6VKUq6t2l4Pxp2ts",
      }
    },
  },
  typechain: {
    target: "ethers-v5",
    alwaysGenerateOverloads: false,
  },
};
