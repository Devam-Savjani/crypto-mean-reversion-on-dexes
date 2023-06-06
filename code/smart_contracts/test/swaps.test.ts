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

        await weth.approve(swapsContract.address, ethers.utils.parseEther('99999999999999999'));
        await dai.approve(swapsContract.address, ethers.utils.parseEther('99999999999999999'));
    });

    describe('Swap WETH/ETH', () => {
        it('should get weth from eth', async () => {

            const ethBalanceBefore = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceBefore = Number((await weth.balanceOf(user!.address))._hex);

            const swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther('10')
            })
            swapEthForWETH.wait()

            const ethBalanceAfter = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceAfter = Number((await weth.balanceOf(user!.address))._hex);

            expect(wethBalanceBefore).to.lessThan(wethBalanceAfter)
            expect(ethBalanceAfter).to.lessThan(ethBalanceBefore)

        })

        it('should get eth from weth', async () => {

            const swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther('10')
            })
            swapEthForWETH.wait()

            const ethBalanceBefore = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceBefore = Number((await weth.balanceOf(user!.address))._hex);

            const swapWethForEth = await swapsContract.swapWethForEth(ethers.utils.parseEther('5'));
            swapWethForEth.wait()

            const ethBalanceAfter = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceAfter = Number((await weth.balanceOf(user!.address))._hex);

            expect(wethBalanceAfter).to.lessThan(wethBalanceBefore)
            expect(ethBalanceBefore).to.lessThan(ethBalanceAfter)

        })
    })

    describe('Swap on Uniswap using Router', () => {
        it('Swap weth for dai', async () => {
            const swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther('10')
            })
            swapEthForWETH.wait()

            const ethBalanceBefore = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceBefore = Number((await weth.balanceOf(user!.address))._hex);
            const daiBalanceBefore = Number((await dai.balanceOf(user!.address))._hex);

            const swapWethForDai = await swapsContract.swapExactUsingRouter(wethAddress, daiAddress, 3000, ethers.utils.parseEther('5'));
            swapWethForDai.wait()

            const ethBalanceAfter = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceAfter = Number((await weth.balanceOf(user!.address))._hex);
            const daiBalanceAfter = Number((await dai.balanceOf(user!.address))._hex);

            expect(ethBalanceAfter).to.lessThan(ethBalanceBefore)
            expect(wethBalanceAfter).to.lessThan(wethBalanceBefore)
            expect(daiBalanceBefore).to.lessThan(daiBalanceAfter)
        })
    })

    describe('Swap on Uniswap using Liquidity Pool', () => {
        it('Swap weth for dai', async () => {
            const swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther('10')
            })
            swapEthForWETH.wait()

            const ethBalanceBefore = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceBefore = Number((await weth.balanceOf(user!.address))._hex);
            const daiBalanceBefore = Number((await dai.balanceOf(user!.address))._hex);

            const wethDaiLiquidityPoolAddress = "0x60594a405d53811d3BC4766596EFD80fd545A270";

            const swapWethForDai = await swapsContract.swapExactUsingPool(wethDaiLiquidityPoolAddress, false, ethers.utils.parseEther('5'))
            swapWethForDai.wait()

            const ethBalanceAfter = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceAfter = Number((await weth.balanceOf(user!.address))._hex);
            const daiBalanceAfter = Number((await dai.balanceOf(user!.address))._hex);

            expect(ethBalanceAfter).to.lessThan(ethBalanceBefore)
            expect(wethBalanceAfter).to.lessThan(wethBalanceBefore)
            expect(daiBalanceBefore).to.lessThan(daiBalanceAfter)
        })
    })

    describe('Aave Depositing, Lending and Repaying', () => {
        it('Deposit WETH', async () => {
            const swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther('10')
            })
            swapEthForWETH.wait()

            const ethBalanceBefore = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceBefore = Number((await weth.balanceOf(user!.address))._hex);
            const daiBalanceBefore = Number((await dai.balanceOf(user!.address))._hex);

            const deposit = await swapsContract.depositCollateral(ethers.utils.parseEther('2'), { gasLimit: 800000 })
            deposit.wait()

            const ethBalanceAfter = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceAfter = Number((await weth.balanceOf(user!.address))._hex);
            const daiBalanceAfter = Number((await dai.balanceOf(user!.address))._hex);

            expect(ethBalanceAfter).to.lessThan(ethBalanceBefore)
            expect(wethBalanceAfter).to.lessThan(wethBalanceBefore)
            expect(daiBalanceAfter).to.equal(daiBalanceBefore)

        })

        it('Borrow Dai', async () => {
            const swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther('10')
            })
            swapEthForWETH.wait()

            const ethBalanceBefore = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceBefore = Number((await weth.balanceOf(user!.address))._hex);
            const daiBalanceBefore = Number((await dai.balanceOf(user!.address))._hex);

            const borrowDai = await swapsContract.borrowToken(daiAddress, ethers.utils.parseEther('10'), ethers.utils.parseEther('2'), { gasLimit: 800000 })
            borrowDai.wait()

            const ethBalanceAfter = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceAfter = Number((await weth.balanceOf(user!.address))._hex);
            const daiBalanceAfter = Number((await dai.balanceOf(user!.address))._hex);

            expect(ethBalanceAfter).to.lessThan(ethBalanceBefore)
            expect(wethBalanceAfter).to.lessThan(wethBalanceBefore)
            expect(daiBalanceBefore).to.lessThan(daiBalanceAfter)
        })

        it('Borrow Dai and Repay Loan', async () => {
            const swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther('10')
            })
            swapEthForWETH.wait()

            const ethBalanceBefore = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceBefore = Number((await weth.balanceOf(user!.address))._hex);
            const daiBalanceBefore = Number((await dai.balanceOf(user!.address))._hex);

            const borrowAmount = ethers.utils.parseEther('10')
            const collateralAmount = ethers.utils.parseEther('1')

            const borrowDai = await swapsContract.borrowToken(daiAddress, borrowAmount, collateralAmount, { gasLimit: 800000 })
            borrowDai.wait()

            const ethBalanceMiddle = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceMiddle = Number((await weth.balanceOf(user!.address))._hex);
            const daiBalanceMiddle = Number((await dai.balanceOf(user!.address))._hex);

            expect(ethBalanceMiddle).to.lessThan(ethBalanceBefore)
            expect(wethBalanceMiddle).to.lessThan(wethBalanceBefore)
            expect(daiBalanceBefore).to.lessThan(daiBalanceMiddle)

            const repayBorrowedDai = await swapsContract.repayBorrowedToken(daiAddress, borrowAmount, collateralAmount, { gasLimit: 800000 })
            repayBorrowedDai.wait()

            const ethBalanceAfter = Number((await ethers.provider.getBalance(user!.address))._hex);
            const wethBalanceAfter = Number((await weth.balanceOf(user!.address))._hex);
            const daiBalanceAfter = Number((await dai.balanceOf(user!.address))._hex);

            expect(ethBalanceAfter).to.lessThan(ethBalanceMiddle)
            expect(wethBalanceMiddle).to.lessThan(wethBalanceAfter)
            expect(wethBalanceAfter).to.equal(wethBalanceBefore)
            expect(daiBalanceAfter).to.equal(daiBalanceBefore)
        })
    })
})