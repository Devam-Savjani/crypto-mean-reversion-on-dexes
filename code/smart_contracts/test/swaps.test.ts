import { SignerWithAddress } from "@nomiclabs/hardhat-ethers/signers";
import chai, { expect } from "chai";
import { solidity } from "ethereum-waffle";
import { ethers } from "hardhat";
import { IWETH, Swaps, Swaps__factory as swapsFactory } from "../typechain";
import { Contract } from "ethers";

chai.use(solidity);

describe("Mint4CareNFT", () => {
    let swapsContract: Swaps;
    let user: SignerWithAddress | undefined;
    let weth: IWETH;

    before(async () => {
        const SwapsContract = await ethers.getContractFactory("Swaps");
        const wethAddress = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        swapsContract = await SwapsContract.deploy(wethAddress);
        await swapsContract.deployed();
        const wethABI = require('../contracts/WETH.json');
        [user] = await ethers.getSigners();
        // weth = new ethers.Contract(wethAddress, wethABI, user)

        weth = await ethers.getContractAt("IWETH", wethAddress, user)
    });

    describe('Swap Weth', ()=> {
        it('should get weth', async () => {
            console.log(ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
            console.log(ethers.utils.formatEther(await weth.balanceOf(user!.address)))

            

            await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther('0.1')
            })
            console.log(ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
            console.log(ethers.utils.formatEther(await weth.balanceOf(user!.address)))


        })
    })
})