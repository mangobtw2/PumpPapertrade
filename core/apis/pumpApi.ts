import { AccountRole, getU64Codec, getStructCodec, KeyPairSigner, signTransactionMessageWithSigners, appendTransactionMessageInstructions, getSignatureFromTransaction, getU8Codec, addSignersToTransactionMessage } from "@solana/web3.js";
import { Address } from "@solana/addresses";
import * as core from "../core.js";
import * as coreUtils from "../coreUtils.js";
import { createLogger } from "../../logger.js";
import * as constants from "../constants.js";
import { Trade } from "../tradeTypes.js";
const logger = createLogger(import.meta.url);


const pumpFunBuyCodec = getStructCodec([
    ["discriminator", getU64Codec()],
    ["amount", getU64Codec()],
    ["maxSolCost", getU64Codec()]
]);

const pumpFunSellCodec = getStructCodec([
    ["discriminator", getU64Codec()],
    ["amount", getU64Codec()],
    ["minSolOutput", getU64Codec()]
]);

export async function buyPumpFun({fromKeypair, mint, amountTokens, maxSolCost, priorityFeeLamports, jitoTipLamports, nozomiTipLamports}: {fromKeypair: KeyPairSigner, mint: Address, amountTokens: bigint, maxSolCost: bigint, priorityFeeLamports: number, jitoTipLamports: number, nozomiTipLamports: number}): Promise<Trade | undefined> {
    try {
        const transaction = await core.createTransaction({feePayer: fromKeypair, priorityFeeLamports, estimatedComputeUnits: 62000, jitoTipLamports, nozomiTipLamports});  
        const buyInstruction = await getPumpFunBuyInstruction({fromKeypair, mint, amountTokens, maxSolCost});
        const idempotentInstruction = await coreUtils.getCreateIdempotentInstruction({fromKeypair, mint});
        const transactionWithInstruction = appendTransactionMessageInstructions([idempotentInstruction, buyInstruction], transaction);
        const signedTransaction = await signTransactionMessageWithSigners(addSignersToTransactionMessage([fromKeypair], transactionWithInstruction));
    
        logger.debug("Buying "+amountTokens+" tokens of "+mint.toString()+ ", tx signature "+getSignatureFromTransaction(signedTransaction));
        const trades = await (nozomiTipLamports > 0 ? core.sendAndConfirmPerformanceWithNozomi : (jitoTipLamports > 0 ? core.sendAndConfirmPerformanceWithJito : core.sendAndConfirmPerformance))(signedTransaction, {commitment: "confirmed", skipPreflight: true});
        if(!trades || trades.length === 0){
            logger.error("Error buying on PumpFun: no trades returned by send and confirm, but no error thrown");
            return undefined;
        } else if(trades.length > 1){
            logger.error("Error buying on PumpFun: multiple trades returned by send and confirm");
            return undefined;
        }
        logger.debug("Buy Transaction "+getSignatureFromTransaction(signedTransaction)+" sent and confirmed!");
        return trades[0];
    } catch (error) {
        if (
            error && 
            typeof error === 'object' && 
            'context' in error && 
            error.context && 
            typeof error.context === 'object'
        ) {
            if('code' in error.context){
                const errorCode = error.context.code as number;
                const errorMessage = getPumpFunErrorFromCode(errorCode);
                if (errorMessage) {
                    logger.warn("Error buying on PumpFun: "+errorMessage);
                    return;
                }
            } else if ('__code' in error.context){
                const errorCode = error.context.__code as number;
                const errorMessage = getPumpFunErrorFromCode(errorCode);
                if (errorMessage) {
                    logger.warn("Error buying on PumpFun: "+errorMessage);
                    return;
                }
            }
            else if(
                error &&
                typeof error === 'object' &&
                'InstructionError' in error &&
                error.InstructionError &&
                typeof error.InstructionError === 'object' &&
                Array.isArray(error.InstructionError) &&
                error.InstructionError.length === 2 &&
                typeof error.InstructionError[1] === 'object' &&
                'Custom' in error.InstructionError[1] &&
                typeof error.InstructionError[1].Custom === 'number'
            ){
                const errorCode = error.InstructionError[1].Custom;
                const errorMessage = getPumpFunErrorFromCode(errorCode);
                if (errorMessage) {
                    logger.warn("Error buying on PumpFun: "+errorMessage);
                    throw errorCode;
                }
            }
        }
        logger.warn("Error creating, sending and confirming buy transaction:", error);
        throw error;
    }
}

export async function sellPumpFun({fromKeypair, mint, amountTokens, minSolOutput, priorityFeeLamports, jitoTipLamports, nozomiTipLamports}: {fromKeypair: KeyPairSigner, mint: Address, amountTokens: bigint, minSolOutput: bigint, priorityFeeLamports: number, jitoTipLamports: number, nozomiTipLamports: number}): Promise<Trade | undefined> {
    try {
        const transaction = await core.createTransaction({feePayer: fromKeypair, priorityFeeLamports, estimatedComputeUnits: 38000, jitoTipLamports, nozomiTipLamports});  
        const sellInstruction = await getPumpFunSellInstruction({fromKeypair, mint, amountTokens, minSolOutput});
        const idempotentInstruction = await coreUtils.getCreateIdempotentInstruction({fromKeypair, mint});
        const transactionWithInstruction = appendTransactionMessageInstructions([idempotentInstruction, sellInstruction], transaction);
        const signedTransaction = await signTransactionMessageWithSigners(addSignersToTransactionMessage([fromKeypair], transactionWithInstruction));

        logger.debug("Selling "+amountTokens+" tokens of "+mint.toString()+ ", tx signature "+getSignatureFromTransaction(signedTransaction));
        const trades = await (nozomiTipLamports > 0 ? core.sendAndConfirmPerformanceWithNozomi : (jitoTipLamports > 0 ? core.sendAndConfirmPerformanceWithJito : core.sendAndConfirmPerformance))(signedTransaction, {commitment: "confirmed", skipPreflight: true});
        if(!trades || trades.length === 0){
            logger.error("Error selling on PumpFun: no trades returned by send and confirm, but no error thrown");
            return undefined;
        } else if(trades.length > 1){
            logger.error("Error selling on PumpFun: multiple trades returned by send and confirm");
            return undefined;
        }
        logger.debug("Sell Transaction "+getSignatureFromTransaction(signedTransaction)+" sent and confirmed!");
        return trades[0];
    } catch (error) {
        if (
            error && 
            typeof error === 'object' && 
            'context' in error && 
            error.context && 
            typeof error.context === 'object'
        ) {
            if('code' in error.context){
                const errorCode = error.context.code as number;
                const errorMessage = getPumpFunErrorFromCode(errorCode);
                if (errorMessage) {
                    logger.warn("Error selling on PumpFun: "+errorMessage);
                    return;
                }
            } else if ('__code' in error.context){
                const errorCode = error.context.__code as number;
                const errorMessage = getPumpFunErrorFromCode(errorCode);
                if (errorMessage) {
                    logger.warn("Error selling on PumpFun: "+errorMessage);
                    return;
                }
            }
        }else if(
            error &&
            typeof error === 'object' &&
            'InstructionError' in error &&
            error.InstructionError &&
            typeof error.InstructionError === 'object' &&
            Array.isArray(error.InstructionError) &&
            error.InstructionError.length === 2 &&
            typeof error.InstructionError[1] === 'object' &&
            'Custom' in error.InstructionError[1] &&
            typeof error.InstructionError[1].Custom === 'number'
        ){
            const errorCode = error.InstructionError[1].Custom;
            const errorMessage = getPumpFunErrorFromCode(errorCode);
            if (errorMessage) {
                logger.warn("Error selling on PumpFun: "+errorMessage);
                throw errorCode;
            }
        }
        logger.warn("Error sending and confirming sell transaction:", error);
        throw error;
    }
}

async function getPumpFunBuyInstruction({fromKeypair, mint, amountTokens, maxSolCost}: {fromKeypair: KeyPairSigner, mint: Address, amountTokens: bigint, maxSolCost: bigint}){
    const tokenAccount = await coreUtils.getAssociatedTokenAddress({owner: fromKeypair.address, mint});
    const bondingCurve = await coreUtils.getBondingCurveAddress({mintPubkey: mint});
    const associatedBondingCurve = await coreUtils.getAssociatedBondingCurveAddress({mintPubkey: mint, bondingCurve});
    return {
        programAddress: constants.PUMP_FUN_PROGRAM_ADDRESS,
        accounts: [
            {
                address: constants.PUMP_FUN_GLOBAL_ADDRESS,
                role: AccountRole.READONLY
            },
            {
                address: constants.PUMP_FUN_FEE_RECEPIENT,
                role: AccountRole.WRITABLE
            },
            {
                address: mint,
                role: AccountRole.READONLY
            },
            {
                address: bondingCurve,
                role: AccountRole.WRITABLE
            },
            {
                address: associatedBondingCurve,
                role: AccountRole.WRITABLE
            },
            {
                address: tokenAccount,
                role: AccountRole.WRITABLE
            },
            {
                address: fromKeypair.address,
                role: AccountRole.WRITABLE_SIGNER
            },
            {
                address: constants.SYSTEM_PROGRAM_ADDRESS,
                role: AccountRole.READONLY
            },
            {
                address: constants.TOKEN_PROGRAM_ADDRESS,
                role: AccountRole.READONLY
            },
            {
                address: constants.RENT_PROGRAM_ADDRESS,
                role: AccountRole.READONLY
            },
            {
                address: constants.PUMP_FUN_EVENT_AUTHORITY,
                role: AccountRole.READONLY
            },
            {
                address: constants.PUMP_FUN_PROGRAM_ADDRESS,
                role: AccountRole.READONLY
            }

        ],
        data: pumpFunBuyCodec.encode({
            discriminator: 16927863322537952870n,
            amount: amountTokens,
            maxSolCost: maxSolCost
        }) as Uint8Array
    }
}

async function getPumpFunSellInstruction({fromKeypair, mint, amountTokens, minSolOutput}: {fromKeypair: KeyPairSigner, mint: Address, amountTokens: bigint, minSolOutput: bigint}){
    const tokenAccount = await coreUtils.getAssociatedTokenAddress({owner: fromKeypair.address, mint});
    const bondingCurve = await coreUtils.getBondingCurveAddress({mintPubkey: mint});
    const associatedBondingCurve = await coreUtils.getAssociatedBondingCurveAddress({mintPubkey: mint, bondingCurve});
    return {
        programAddress: constants.PUMP_FUN_PROGRAM_ADDRESS,
        accounts: [
            {
                address: constants.PUMP_FUN_GLOBAL_ADDRESS,
                role: AccountRole.READONLY
            },
            {
                address: constants.PUMP_FUN_FEE_RECEPIENT,
                role: AccountRole.WRITABLE
            },
            {
                address: mint,
                role: AccountRole.READONLY
            },
            {
                address: bondingCurve,
                role: AccountRole.WRITABLE
            },
            {
                address: associatedBondingCurve,
                role: AccountRole.WRITABLE
            },
            {
                address: tokenAccount,
                role: AccountRole.WRITABLE
            },
            {
                address: fromKeypair.address,
                role: AccountRole.WRITABLE_SIGNER
            },
            {
                address: constants.SYSTEM_PROGRAM_ADDRESS,
                role: AccountRole.READONLY
            },
            {
                address: constants.ASSOCIATED_TOKEN_PROGRAM_ADDRESS,
                role: AccountRole.READONLY
            },
            {
                address: constants.TOKEN_PROGRAM_ADDRESS,
                role: AccountRole.READONLY
            },
            {
                address: constants.PUMP_FUN_EVENT_AUTHORITY,
                role: AccountRole.READONLY
            },
            {
                address: constants.PUMP_FUN_PROGRAM_ADDRESS,
                role: AccountRole.READONLY
            }

        ],
        data: pumpFunSellCodec.encode({
            discriminator: 12502976635542562355n,
            amount: amountTokens,
            minSolOutput: minSolOutput
        }) as Uint8Array
    }
}

function getPumpFunErrorFromCode(code: number){
    if (code == 1){
        return "timed out: blockhash expired"
    } else if (code === 6002) {
        return "buy slippage exceeded";
    } else if (code === 6003) {
        return "sell slippage exceeded";
    } else if (code === 6004) {
        return "mint does not match bonding curve";
    } else if (code === 6005) {
        return "coin has migrated";
    } else if (code == 4615041) {
        return "possibly insufficient funds"
    }
    return undefined;
}