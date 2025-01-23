import { signTransactionMessageWithSigners, getSignatureFromTransaction, appendTransactionMessageInstruction, appendTransactionMessageInstructions, AccountRole } from "@solana/web3.js";
import { KeyPairSigner, Address } from "@solana/web3.js";
import { getTransferSolInstruction } from '@solana-program/system';
import { getSyncNativeInstruction, getCloseAccountInstruction } from '@solana-program/token';
import { createLogger } from "../../logger.js";
import * as coreUtils from '../coreUtils.js'
import * as constants from '../constants.js'
import {rpc} from "../index.js"

const logger = createLogger(import.meta.url);

import * as core from "../core.js";


export async function transferSol({fromKeypair, to, solAmount, jito}: {fromKeypair: KeyPairSigner, to: Address, solAmount: number, jito: boolean}) {
    const lamports = solAmount * 10 ** 9;
    const transaction = await core.createTransaction({feePayer: fromKeypair, priorityFeeLamports: 10000, estimatedComputeUnits: 150, jitoTipLamports: jito ? 10000 : 0, nozomiTipLamports: 0});
    const transferInstruction = getTransferSolInstruction({
        amount: lamports,
        destination: to,
        source: fromKeypair
    });
    const transactionWithTransfer = appendTransactionMessageInstruction(transferInstruction, transaction);
    const signedTransaction = await signTransactionMessageWithSigners(transactionWithTransfer);
    logger.debug("Sending sol transfer transaction", getSignatureFromTransaction(signedTransaction));
    try {
        await core.sendAndConfirmPerformance(signedTransaction, {commitment: "confirmed", skipPreflight: true});
        logger.debug("sol transfer transaction "+getSignatureFromTransaction(signedTransaction)+" sent and confirmed!");
        return true;
    } catch (error) {
        logger.warn("Error sending and confirming sol transfer transaction "+getSignatureFromTransaction(signedTransaction)+":", error);
        return false;
    }
}

export async function wrapSol(keypair: KeyPairSigner, lamports: number){
    for(let i = 0;i<1;i++){
        const instructions = await getWrapInstructions(keypair, lamports);
        const transaction = await core.createTransaction({feePayer: keypair, priorityFeeLamports: 0, estimatedComputeUnits: 40000, jitoTipLamports: 5100, nozomiTipLamports: 0});
        const transactionWithInstructions = appendTransactionMessageInstructions(instructions, transaction);
        const signedTransaction = await signTransactionMessageWithSigners(transactionWithInstructions);
        try{
            await core.sendAndConfirmPerformanceWithJito(signedTransaction, {commitment: "confirmed", skipPreflight: true});
            return;
        } catch(error){
            logger.warn("Error sending and confirming wrap sol transaction "+getSignatureFromTransaction(signedTransaction)+":", error);
        }
    }
    logger.error("Failed to send and confirm wrap sol transaction after 3 attempts");
    return;
}

export async function unwrapSol(keypair: KeyPairSigner, lamports: number){
    try{
        const wsolATA = await coreUtils.getAssociatedTokenAddress({owner: keypair.address, mint: constants.WSOL_ADDRESS});
        const currentWsolBalance = await rpc.performanceRpc.getTokenAccountBalance(wsolATA).send();
        const needToWrapLamports = Number(currentWsolBalance.value.amount) - lamports;
        if(needToWrapLamports > 0){
            for(let i = 0;i<1;i++){
                try{
                    const unwrapInstruction = await getUnwrapInstruction(keypair);
                    const wrapInstructions = await getWrapInstructions(keypair, needToWrapLamports);
                    const transaction = await core.createTransaction({feePayer: keypair, priorityFeeLamports: 0, estimatedComputeUnits: 60000, jitoTipLamports: 5100, nozomiTipLamports: 0});
                    const transactionWithInstructions = appendTransactionMessageInstructions([unwrapInstruction, ...wrapInstructions], transaction);
                    const signedTransaction = await signTransactionMessageWithSigners(transactionWithInstructions);
                    await core.sendAndConfirmPerformanceWithJito(signedTransaction, {commitment: "confirmed", skipPreflight: true});
                    return;
                }catch(error){
                    logger.warn("Error sending and confirming unwrap sol transaction:", error);
                }
            }
            logger.error("Failed to send and confirm unwrap sol transaction after 3 attempts");
            return;
        }
    } catch(error){
        logger.warn("Wsol balance empty or error getting wsol balance: ", error);
        return;
    }
}


async function getUnwrapInstruction(keypair: KeyPairSigner){
    const wsolATA = await coreUtils.getAssociatedTokenAddress({owner: keypair.address, mint: constants.WSOL_ADDRESS});
    const closeAccountInstruction = getCloseAccountInstruction({
        account: wsolATA,
        destination: keypair.address,
        owner: keypair.address,
    });
    return closeAccountInstruction;
}

async function getWrapInstructions(keypair: KeyPairSigner, lamports: number){
    const wsolATA = await coreUtils.getAssociatedTokenAddress({owner: keypair.address, mint: constants.WSOL_ADDRESS});
    const createIdempotentInstruction = await coreUtils.getCreateIdempotentInstruction({fromKeypair: keypair, mint: constants.WSOL_ADDRESS});
    const transferInstruction = getTransferSolInstruction({
        amount: lamports,
        destination: wsolATA,
        source: keypair
    });
    const syncNativeInstruction = getSyncNativeInstruction({
        account: wsolATA
    });
    return [createIdempotentInstruction, transferInstruction, syncNativeInstruction];
}