import { SignerWithAddress } from "@nomiclabs/hardhat-ethers/signers";
import chai, { expect } from "chai";
import { solidity } from "ethereum-waffle";
import { ethers } from "hardhat";
import { IERC20, IWETH, Swaps } from "../typechain";
import { Address } from "cluster";

chai.use(solidity);

describe("Swaps", () => {
    let swapsContract: Swaps;
    let user: SignerWithAddress | undefined;
    let weth: IWETH;
    let dai: IERC20;
    let wethAddress: string;
    let daiAddress: string;

    before(async () => {
        const SwapsContract = await ethers.getContractFactory("Swaps");
        wethAddress = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        daiAddress = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
        swapsContract = await SwapsContract.deploy();
        await swapsContract.deployed();
        [user] = await ethers.getSigners();
        weth = await ethers.getContractAt("IWETH", wethAddress, user);
        dai = await ethers.getContractAt("IERC20", daiAddress, user);
    });

    describe('Swap Weth', ()=> {
        it('should get weth', async () => {
            const swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther('10')
            })
            swapEthForWETH.wait()
        
            console.log('ETH BEFORE ',ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
            console.log('WETH BEFORE ',ethers.utils.formatEther(await weth.balanceOf(user!.address)))
            console.log('DAI BEFORE ',ethers.utils.formatEther(await dai.balanceOf(user!.address)))
            console.log()
        
            // Swap WETH for DAI
            await weth.approve(swapsContract.address, ethers.utils.parseEther('1'))
            const swap = await swapsContract.swapExactUsingRouter(wethAddress, daiAddress, 3000, ethers.utils.parseEther('0.1'), { gasLimit: 300000 })
            swap.wait()
        
            console.log('ETH AFTER ',ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
            console.log('WETH AFTER ',ethers.utils.formatEther(await weth.balanceOf(user!.address)))
            console.log('DAI AFTER ',ethers.utils.formatEther(await dai.balanceOf(user!.address)))

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