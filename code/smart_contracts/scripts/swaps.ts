import { ethers } from "hardhat";
import { IERC20, IUniswapV3Pool, IWETH, Swaps } from "../typechain";
import { SignerWithAddress } from "@nomiclabs/hardhat-ethers/signers";
import { BigNumber } from "ethers";

// npx hardhat node --fork https://eth-mainnet.g.alchemy.com/v2/F5gSnV_OJ77nOQWI6VKUq6t2l4Pxp2ts --hostname 127.0.0.1
// npx hardhat run scripts/swaps.ts
async function main() {
    let swapsContract: Swaps;
    let user: SignerWithAddress | undefined;
    let weth: IWETH;
    let dai: IERC20;
    let pool: IUniswapV3Pool;

    const wethAddress = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    const daiAddress = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
    const routerAddress = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
    const poolAddress = "0x60594a405d53811d3BC4766596EFD80fd545A270"

    const SwapsContract = await ethers.getContractFactory("Swaps");
    swapsContract = await SwapsContract.deploy();
    await swapsContract.deployed();
    [user] = await ethers.getSigners();
    weth = await ethers.getContractAt("IWETH", wethAddress, user);
    dai = await ethers.getContractAt("IERC20", daiAddress, user);

    await weth.approve(swapsContract.address, ethers.utils.parseEther('99999999999'));
    await dai.approve(swapsContract.address, ethers.utils.parseEther('99999999999'));

    // Swap ETH FOR WETH
    let swapEthForWETH = await swapsContract.swapEthForWeth({
        value: ethers.utils.parseEther('10')
    })
    swapEthForWETH.wait()

    console.log('ETH BEFORE ', ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
    console.log('WETH BEFORE ', ethers.utils.formatEther(await weth.balanceOf(user!.address)))
    console.log('DAI BEFORE ', ethers.utils.formatEther(await dai.balanceOf(user!.address)))
    console.log()


    // Swap WETH FOR ETH
    const swapWethForEth = await swapsContract.swapWethForEth(ethers.utils.parseEther('1'), { gasLimit: 3000000 });
    swapWethForEth.wait()

    console.log('WETH BEFORE in smart contract ', ethers.utils.formatEther(await weth.balanceOf(swapsContract.address)))
    console.log('ETH BEFORE in smart contract ', ethers.utils.formatEther(await ethers.provider.getBalance(swapsContract.address)))
    console.log('ETH BEFORE ', ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
    console.log('WETH BEFORE ', ethers.utils.formatEther(await weth.balanceOf(user!.address)))
    console.log('DAI BEFORE ', ethers.utils.formatEther(await dai.balanceOf(user!.address)))
    console.log()

    // Swap WETH for DAI
    const swap = await swapsContract.swapExactUsingPool(poolAddress, false, ethers.utils.parseEther('0.1'), { gasLimit: 300000 })
    swap.wait()

    console.log('ETH AFTER ', ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
    console.log('WETH AFTER ', ethers.utils.formatEther(await weth.balanceOf(user!.address)))
    console.log('DAI AFTER ', ethers.utils.formatEther(await dai.balanceOf(user!.address)))
    console.log()

    const deposit = await swapsContract.borrowToken(daiAddress, ethers.utils.parseEther('10'), ethers.utils.parseEther('2'), { gasLimit: 800000 })
    deposit.wait()

    console.log('ETH AFTER ', ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
    console.log('WETH AFTER ', ethers.utils.formatEther(await weth.balanceOf(user!.address)))
    console.log('DAI AFTER ', ethers.utils.formatEther(await dai.balanceOf(user!.address)))
    console.log()

    const repay = await swapsContract.repayBorrowedToken(daiAddress, ethers.utils.parseEther('10'), ethers.utils.parseEther('2'), { gasLimit: 800000 })
    repay.wait()

    console.log('ETH AFTER ', ethers.utils.formatEther(await ethers.provider.getBalance(user!.address)))
    console.log('WETH AFTER ', ethers.utils.formatEther(await weth.balanceOf(user!.address)))
    console.log('DAI AFTER ', ethers.utils.formatEther(await dai.balanceOf(user!.address)))

    const values = ['0.1', '0.5', '1', '5', '10', '15', '20', '25', '30', '35', '40', '45', '50', '55', '60', '65', '70', '75', '80', '85', '90', '100', '110', '120', '138', '150'];
    
    let gasUsedUsingRouter: Number[] = []
    for (let index = 0; index < values.length; index++) {
        let amount = values[index];
        
        let swapEthForWETH = await swapsContract.swapEthForWeth({
            value: ethers.utils.parseEther(amount!)
        })
        
        swapEthForWETH.wait();
        let swapWethForDai = await swapsContract.swapExactUsingRouter(wethAddress, daiAddress, 3000, ethers.utils.parseEther(amount!));
        let receipt = swapWethForDai.wait()
        await gasUsedUsingRouter.push(Number((await receipt).gasUsed._hex))
    }
    console.log(gasUsedUsingRouter)
    
    let gasUsedUsingPool: Number[] = []
    const wethDaiLiquidityPoolAddress = "0x60594a405d53811d3BC4766596EFD80fd545A270";
    for (let index = 0; index < values.length; index++) {
        let amount = values[index];
        
        let swapEthForWETH = await swapsContract.swapEthForWeth({
            value: ethers.utils.parseEther(amount!)
        })
        swapEthForWETH.wait();

        let swapWethForDai = await swapsContract.swapExactUsingPool(wethDaiLiquidityPoolAddress, false, ethers.utils.parseEther(amount!));
        let receipt = swapWethForDai.wait()
        await gasUsedUsingPool.push(Number((await receipt).gasUsed._hex))
    }
    console.log(gasUsedUsingPool)

    let gasUsedDeposit: Number[] = []
    let gasUsedWithdraw: Number[] = []
    for (let index = 0; index < values.length; index++) {
        let amount = values[index];
        
        let swapEthForWETH = await swapsContract.swapEthForWeth({
            value: ethers.utils.parseEther(amount!)
        })

        swapEthForWETH.wait();
        let deposit = await swapsContract.depositCollateral(ethers.utils.parseEther(amount!));
        let depositReceipt = deposit.wait()
        await gasUsedDeposit.push(Number((await depositReceipt).gasUsed._hex))

        let withdraw = await swapsContract.withdrawCollateral(ethers.utils.parseEther(amount!))
        let withdrawReceipt = withdraw.wait()
        await gasUsedWithdraw.push(Number((await withdrawReceipt).gasUsed._hex))
    }
    console.log(gasUsedDeposit)
    console.log(gasUsedWithdraw)

    let gasUsedBorrow: Number[][] = []
    let gasUsedRepay: Number[][] = []

    let collateralAmounts = ['5', '30', '66', '100'];
    let borrowAmounts = ['0.5', '5', '10', '66', '100', '250', '500', '750', '1000', '1500', '2000'];

    for (let withdrawIndex = 0; withdrawIndex < collateralAmounts.length; withdrawIndex++) {
        let gasUsedBorrowTemp : Number[] = []
        let gasUsedRepayTemp : Number[] = []
        for (let borrowIndex = 0; borrowIndex < borrowAmounts.length; borrowIndex++) {
        
            let swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther(collateralAmounts[withdrawIndex]!)
            })
            swapEthForWETH.wait();
    
            let borrowDai = await swapsContract.borrowToken(daiAddress, ethers.utils.parseEther(borrowAmounts[borrowIndex]!), ethers.utils.parseEther(collateralAmounts[withdrawIndex]!));
            let borrowReceipt = borrowDai.wait();
            await gasUsedBorrowTemp.push(Number((await borrowReceipt).gasUsed._hex))

            let repayBorrowedDai = await swapsContract.repayBorrowedToken(daiAddress, ethers.utils.parseEther(borrowAmounts[borrowIndex]!), ethers.utils.parseEther(collateralAmounts[withdrawIndex]!));
            let repayReceipt = repayBorrowedDai.wait();
            await gasUsedRepayTemp.push(Number((await repayReceipt).gasUsed._hex))
        }
        gasUsedBorrow.push(gasUsedBorrowTemp)
        gasUsedRepay.push(gasUsedRepayTemp)
    }
    console.log(gasUsedBorrow)
    console.log(gasUsedRepay)

    swapEthForWETH = await swapsContract.swapEthForWeth({
        value: (await ethers.provider.getBalance(user!.address)).sub(ethers.utils.parseEther('150'))
    })
    swapEthForWETH.wait()

    let gasUsedOpen: Number[] = []
    let gasUsedClose: Number[] = []

    collateralAmounts = ['30', '66', '100'];
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
                console.log('Buy + Close, Collateral=' + collateralAmountStr + ' BuyAmount=' + buyAmountStr + ' SellAmount=' + sellAmountStr)
                let buyAmount = ethers.utils.parseEther(buyAmountStr);
                let sellAmount = ethers.utils.parseEther(sellAmountStr);
                let collatoralAmount = ethers.utils.parseEther(collateralAmountStr);
                let wethBalanceBeforeBigInt: BigNumber = await weth.balanceOf(user!.address);
                let daiBalanceBeforeBigInt: BigNumber = await dai.balanceOf(user!.address);

                let openTrade = await swapsContract.openBuySellPositions(buyPool, buyZeroForOne, buyAmount, sellTokenAddress, sellAmount, collatoralAmount, sellPool, sellZeroForOne);
                let openReceipt = openTrade.wait()
                gasUsedOpen.push(Number((await openReceipt).gasUsed._hex))

                let daiAdded = (await dai.balanceOf(user!.address)).sub(daiBalanceBeforeBigInt)
                let wethAdded = (await weth.balanceOf(user!.address)).add(buyAmount).add(collatoralAmount).sub(wethBalanceBeforeBigInt)

                let closeTrade = await swapsContract.closeBuySellPositions(buyPool, !buyZeroForOne, daiAdded, sellTokenAddress, wethAdded, collatoralAmount.div(ethers.utils.parseEther('2')), sellPool, !sellZeroForOne);
                let closeReceipt =closeTrade.wait()
                gasUsedClose.push(Number((await closeReceipt).gasUsed._hex))
            }
        }
    }

    console.log(gasUsedOpen)
    console.log(gasUsedClose)
}

main()
    .then(() => process.exit(0))
    .catch((error) => {
        console.error(error);
        process.exit(1);
    });