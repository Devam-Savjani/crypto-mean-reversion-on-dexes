import { SignerWithAddress } from "@nomiclabs/hardhat-ethers/signers";
import chai, { expect } from "chai";
import { solidity } from "ethereum-waffle";
import { ethers, network } from "hardhat";
import { IERC20, IWETH, Swaps } from "../typechain";
import { BigNumber } from "ethers";

chai.use(solidity);


describe("Trades to get gas fees", () => {
    let swapsContract: Swaps;
    let user1: SignerWithAddress | undefined;
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

    it('Resets Balance', async () => {
        const swapEthForWETH = await swapsContract.swapEthForWeth({
            value: (await ethers.provider.getBalance(user!.address)).sub(ethers.utils.parseEther('150'))
        })
        swapEthForWETH.wait()
    })

    let collateralAmounts = ['30', '66', '100'];
    let buyAmounts = ['0.00001', '0.0001', '0.001', '0.01','0.1', '1'];
    let sellAmounts = ['10', '66', '100', '250', '500'];

    const buyPool = "0x60594a405d53811d3bc4766596efd80fd545a270";
    const buyZeroForOne = false;
    const sellPool = "0x60594a405d53811d3bc4766596efd80fd545a270";
    const sellZeroForOne = true;
    const sellTokenAddress = "0x6B175474E89094C44Da98b954EedeAC495271d0F";

    for (let collateralIndex = 0; collateralIndex < collateralAmounts.length; collateralIndex++) {
        let collateralAmountStr = collateralAmounts[collateralIndex]!;
        for (let buyIndex = 0; buyIndex < buyAmounts.length; buyIndex++) {
            let buyAmountStr = buyAmounts[buyIndex]!;
            for (let sellIndex = 0; sellIndex < sellAmounts.length; sellIndex++) {
                let sellAmountStr = sellAmounts[sellIndex]!;
                it('Buy + Close, Collateral=' + collateralAmountStr + ' BuyAmount=' + buyAmountStr + ' SellAmount=' + sellAmountStr, async () => {
                    const buyAmount = ethers.utils.parseEther(buyAmountStr);
                    const sellAmount = ethers.utils.parseEther(sellAmountStr);
                    const collatoralAmount = ethers.utils.parseEther(collateralAmountStr);
                    const wethBalanceBeforeBigInt: BigNumber = await weth.balanceOf(user!.address);
                    const daiBalanceBeforeBigInt: BigNumber = await dai.balanceOf(user!.address);

                    const openTrade = await swapsContract.openBuySellPositions(buyPool, buyZeroForOne, buyAmount, sellTokenAddress, sellAmount, collatoralAmount, sellPool, sellZeroForOne);
                    openTrade.wait()

                    const daiAdded = (await dai.balanceOf(user!.address)).sub(daiBalanceBeforeBigInt)
                    const wethAdded = (await weth.balanceOf(user!.address)).add(buyAmount).add(collatoralAmount).sub(wethBalanceBeforeBigInt)

                    const closeTrade = await swapsContract.closeBuySellPositions(buyPool, !buyZeroForOne, daiAdded, sellTokenAddress, wethAdded, collatoralAmount.div(ethers.utils.parseEther('2')), sellPool, !sellZeroForOne);
                    closeTrade.wait()
                })

            }

        }
    }

})