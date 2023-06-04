import { ethers } from "hardhat";
import { IERC20, IWETH, Swaps } from "../typechain";
import { SignerWithAddress } from "@nomiclabs/hardhat-ethers/signers";

// npx hardhat node --fork https://eth-mainnet.g.alchemy.com/v2/F5gSnV_OJ77nOQWI6VKUq6t2l4Pxp2ts --hostname 127.0.0.1
async function main() {
    let swapsContract: Swaps;
    let user: SignerWithAddress | undefined;
    let weth: IWETH;
    let dai: IERC20;

    const wethAddress = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    const daiAddress = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
    const routerAddress = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
    
    const SwapsContract = await ethers.getContractFactory("Swaps");
    swapsContract = await SwapsContract.deploy();
    await swapsContract.deployed();
    [user] = await ethers.getSigners();
    weth = await ethers.getContractAt("IWETH", wethAddress, user);
    dai = await ethers.getContractAt("IERC20", daiAddress, user);

    // Swap ETH FOR WETH
    const swapEthForWETH = await swapsContract.swapEthForWeth({
        value: ethers.utils.parseEther('10')
    })
    swapEthForWETH.wait()

    console.log('ETH BEFORE ',ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
    console.log('WETH BEFORE ',ethers.utils.formatEther(await weth.balanceOf(user!.address)))
    console.log('DAI BEFORE ',ethers.utils.formatEther(await dai.balanceOf(user!.address)))
    
    // Swap WETH for DAI
    // await weth.approve(swapsContract.address, ethers.utils.parseEther('1'))
    // const swap = await swapsContract.swapWETHForDAI(ethers.utils.parseEther('0.1'), { gasLimit: 300000 })
    // swap.wait()

    // Swap WETH for DAI
    await weth.approve(swapsContract.address, ethers.utils.parseEther('1'))
    const swap = await swapsContract.swapExact(wethAddress, daiAddress, 3000, ethers.utils.parseEther('0.1'), { gasLimit: 300000 })
    swap.wait()

    console.log('ETH AFTER ',ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
    console.log('WETH AFTER ',ethers.utils.formatEther(await weth.balanceOf(user!.address)))
    console.log('DAI AFTER ',ethers.utils.formatEther(await dai.balanceOf(user!.address)))
    console.log()
}

main()
    .then(() => process.exit(0))
    .catch((error) => {
        console.error(error);
        process.exit(1);
    });