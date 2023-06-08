import { SignerWithAddress } from "@nomiclabs/hardhat-ethers/signers";
import chai, { expect } from "chai";
import { solidity } from "ethereum-waffle";
import { ethers, network } from "hardhat";
import { IERC20, IWETH, Swaps } from "../typechain";

chai.use(solidity);


describe("Swaps to get gas fees", () => {
    let swapsContract: Swaps;
    let user: SignerWithAddress | undefined;
    let weth: IWETH;
    let dai: IERC20;
    let wethAddress: string;
    let daiAddress: string;
    const values = ['0.1', '0.5', '1', '5', '10', '15', '20', '25', '30', '35', '40', '45', '50', '55', '60', '65', '70', '75', '80', '85', '90', '100', '110', '120', '138', '150'];

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

    for (let index = 0; index < values.length; index++) {
        let amount = values[index];

        it('Swap ' + amount + 'ETH for WETH and vice versa', async () => {
            let swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther(amount!)
            })
            swapEthForWETH.wait();

            swapEthForWETH.wait();

            let swapWethForEth = await swapsContract.swapWethForEth(ethers.utils.parseEther(amount!))
            swapWethForEth.wait()
        })
    }

    for (let index = 0; index < values.length; index++) {
        let amount = values[index];

        it('Swap ' + amount + 'WETH for DAI using uniswap router', async () => {

            let swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther(amount!)
            })

            swapEthForWETH.wait();
            let swapWethForDai = await swapsContract.swapExactUsingRouter(wethAddress, daiAddress, 3000, ethers.utils.parseEther(amount!));
            swapWethForDai.wait()
        })
    }

    for (let index = 0; index < values.length; index++) {
        let amount = values[index];

        it('Swap ' + amount + 'WETH for DAI using uniswap pool', async () => {
            const wethDaiLiquidityPoolAddress = "0x60594a405d53811d3BC4766596EFD80fd545A270";

            let swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther(amount!)
            })

            swapEthForWETH.wait();
            let swapWethForDai = await swapsContract.swapExactUsingPool(wethDaiLiquidityPoolAddress, false, ethers.utils.parseEther(amount!));
            swapWethForDai.wait()
        })
    }

    for (let index = 0; index < values.length; index++) {
        let amount = values[index];

        it('Deposit and Withdraw ' + amount + 'WETH in Aave', async () => {
            let swapEthForWETH = await swapsContract.swapEthForWeth({
                value: ethers.utils.parseEther(amount!)
            })

            swapEthForWETH.wait();
            let deposit = await swapsContract.depositCollateral(ethers.utils.parseEther(amount!));
            deposit.wait()

            let withdraw = await swapsContract.withdrawCollateral(ethers.utils.parseEther(amount!))
            withdraw.wait()
        })
    }

    let collateralAmounts = ['5', '30', '66', '100'];
    let borrowAmounts = ['0.5', '5', '10', '66', '100', '250', '500', '750', '1000', '1500', '2000'];

    for (let withdrawIndex = 0; withdrawIndex < collateralAmounts.length; withdrawIndex++) {
        for (let borrowIndex = 0; borrowIndex < borrowAmounts.length; borrowIndex++) {
        
            it('Borrowing ' + borrowAmounts[borrowIndex] + 'DAI using Aave using ' + collateralAmounts[withdrawIndex] + 'ETH as Collateral', async () => {
                let swapEthForWETH = await swapsContract.swapEthForWeth({
                    value: ethers.utils.parseEther(collateralAmounts[withdrawIndex]!)
                })
                swapEthForWETH.wait();
        
        
                let borrowDai = await swapsContract.borrowToken(daiAddress, ethers.utils.parseEther(borrowAmounts[borrowIndex]!), ethers.utils.parseEther(collateralAmounts[withdrawIndex]!));
                borrowDai.wait();

                let repayBorrowedDai = await swapsContract.repayBorrowedToken(daiAddress, ethers.utils.parseEther(borrowAmounts[borrowIndex]!), ethers.utils.parseEther(collateralAmounts[withdrawIndex]!));
                repayBorrowedDai.wait();
            })
        }
    }
})