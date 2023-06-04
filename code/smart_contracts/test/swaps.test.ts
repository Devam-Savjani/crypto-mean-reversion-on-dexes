import { SignerWithAddress } from "@nomiclabs/hardhat-ethers/signers";
import chai, { expect } from "chai";
import { solidity } from "ethereum-waffle";
import { ethers } from "hardhat";
import { IERC20, IWETH, Swaps } from "../typechain";

chai.use(solidity);

describe("Swaps", () => {
    let swapsContract: Swaps;
    let user: SignerWithAddress | undefined;
    let weth: IWETH;
    let usdc: IERC20;

    before(async () => {
        const SwapsContract = await ethers.getContractFactory("Swaps");
        const wethAddress = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        const usdcAddress = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        swapsContract = await SwapsContract.deploy();
        await swapsContract.deployed();
        [user] = await ethers.getSigners();
        weth = await ethers.getContractAt("IWETH", wethAddress, user);

        usdc = await ethers.getContractAt("IERC20", usdcAddress, user);
    });

    describe('Swap Weth', ()=> {
        it('should get weth', async () => {
            console.log(ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
            console.log(ethers.utils.formatEther(await weth.balanceOf(user!.address)))
            console.log(ethers.utils.formatEther(await usdc.balanceOf(user!.address)))
            console.log()

            await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther('100')
            })

            console.log(ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
            console.log(ethers.utils.formatEther(await weth.balanceOf(user!.address)))
            console.log(ethers.utils.formatEther(await usdc.balanceOf(user!.address)))
            console.log()

            // await swapsContract.swapExactInputSingle(weth.address, link.address, ethers.utils.parseEther('0.5'))
            await swapsContract.swapExactInputSingle(ethers.utils.parseEther('0.5'))
            // await swapsContract.swapExactInputSingleHop(weth.address, usdc.address, 3000, ethers.utils.parseEther('0.5'))
            // await swapsContract.swapExactInputSingleHop(usdc.address, weth.address, 3000, ethers.utils.parseEther('0.5'))

            console.log(ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
            console.log(ethers.utils.formatEther(await weth.balanceOf(user!.address)))
            console.log(ethers.utils.formatEther(await usdc.balanceOf(user!.address)))

        })
    })

    // describe('Swap for link', ()=> {
    //     it('should get weth', async () => {
    //         console.log(ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
    //         console.log(ethers.utils.formatEther(await weth.balanceOf(user!.address)))


    //         // console.log(ethers.utils.formatEther(await link.balanceOf(user!.address)))

    //         await swapsContract.swapEthForWeth({
    //             value: ethers.utils.parseEther('1')
    //         })
    //         console.log(ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
    //         console.log(ethers.utils.formatEther(await weth.balanceOf(user!.address)))

    //     })
    // })
})