import { basicRpc, performanceRpc, websocketRpc} from "./rpc.js";
import * as grpc from "./grpc.js";
import { getTransferSolInstruction } from '@solana-program/system';
import { getSetComputeUnitLimitInstruction, getSetComputeUnitPriceInstruction } from '@solana-program/compute-budget';
import { pipe } from '@solana/functional'
import { addSignersToTransactionMessage, Signature, sendAndConfirmTransactionFactory, sendTransactionWithoutConfirmingFactory, createTransactionMessage, setTransactionMessageFeePayer, KeyPairSigner, Blockhash, setTransactionMessageLifetimeUsingBlockhash, appendTransactionMessageInstruction, Commitment, Transaction, getSignatureFromTransaction, getSignersFromTransactionMessage } from "@solana/web3.js";
import * as jito from "./jito.js";
import * as nozomi from "./nozomi.js";
import * as constants from './constants.js';
import * as config from '../config.js';
import * as coreUtils from './coreUtils.js';
import { createLogger } from '../logger.js';
import { Trade } from "./tradeTypes.js";
import { grpcTransactionToTrades, web3jsTransactionToTrades } from "./coreUtils.js";

const logger = createLogger(import.meta.url);

const CONFIRMATION_TIMEOUT = 60000;


// +++ SEND AND CONFIRM +++

const sendTransactionWithoutConfirmingInternal = sendTransactionWithoutConfirmingFactory({ rpc: performanceRpc });

async function noThrowSendTransactionWithoutConfirmingInternal(...params: Parameters<typeof sendTransactionWithoutConfirmingInternal>){
    try{
        await sendTransactionWithoutConfirmingInternal(...params);
    } catch(error){
        //do nothing
    }
}

export async function sendAndConfirmPerformance(...params: Parameters<typeof sendTransactionWithoutConfirmingInternal>): Promise<Trade[] | undefined>{
    const transaction = params[0];
    const signature = getSignatureFromTransaction(transaction);
    const signer = Object.keys(transaction.signatures)[0];

    
    //here, we want to await potential errors from the sendTransactionWithoutConfirmingInternal function, since this is the only way we submit transaction to the chain
    noThrowSendTransactionWithoutConfirmingInternal(...params);
    const result = await grpc.confirmSignature({signature: signature});
    return result;
}

export async function sendAndConfirmPerformanceWithJito(...params: Parameters<typeof sendTransactionWithoutConfirmingInternal>): Promise<Trade[] | undefined>{
    const transaction = params[0];
    const signature = getSignatureFromTransaction(transaction);
    const signer = Object.keys(transaction.signatures)[0];

    //submit without awaiting. we don't want to await errors from the sendTransactionWithoutConfirmingInternal function, since we also submit to jito
    jito.sendTransaction(transaction);
    //noThrowSendTransactionWithoutConfirmingInternal(...params);

    //const result = await Promise.race([awaitSignatureConfirmationFromRpc({signature: signature as Signature}), awaitSignatureConfirmationFromGrpc({signature: signature, wallet: signer, commitment: "confirmed"})]);
    const result = await grpc.confirmSignature({signature: signature});
    return result;
}

export async function sendAndConfirmPerformanceWithNozomi(...params: Parameters<typeof sendTransactionWithoutConfirmingInternal>): Promise<Trade[] | undefined>{
    const transaction = params[0];
    const signature = getSignatureFromTransaction(transaction);
    const signer = Object.keys(transaction.signatures)[0];

    nozomi.sendTransaction(transaction);
    const result = await grpc.confirmSignature({signature: signature});
    return result;
}

// async function awaitSignatureConfirmationFromRpc({signature}: {signature: Signature}): Promise<Trade[] | undefined> {
//     return new Promise((resolve) => {
//         const interval = setInterval(async () => {
//             const signatureStatus = await performanceRpc.getSignatureStatuses([signature]).send();
//             if(signatureStatus.value[0]?.confirmationStatus == "confirmed" || signatureStatus.value[0]?.confirmationStatus == "finalized"){
//                 clearInterval(interval);
//                 const trades = await getTransactionTrades(signature as Signature);
//                 resolve(trades);
//             }
//         }, 5000);
//     });
// }

// async function awaitSignatureConfirmationFromGrpc({signature, wallet, commitment}: {signature: string, wallet: string, commitment: Commitment & ("confirmed" | "processed")}): Promise<Trade[] | undefined> {
//     let timeoutId: NodeJS.Timeout;
//     let signatureResolve: (data: grpc.SignatureSubscriptionUpdate) => void;

//     //timeoutPromise is always rejected, never resolves
//     const timeoutPromise = new Promise<grpc.SignatureSubscriptionUpdate>((resolve, reject) => {
//         timeoutId = setTimeout(() => {
//             logger.warn("Signature confirmation timeout for signature "+signature);
//             reject(new Error("Signature confirmation timeout"));
//         }, CONFIRMATION_TIMEOUT);
//     });

//     //signaturePromise is resolved by the subscribeSignature callback;
//     const signaturePromise = new Promise<grpc.SignatureSubscriptionUpdate>((resolve, reject) => {
//         signatureResolve = resolve;
//     });
    
//     const unsubscribe = await grpc.subscribeSignature({
//         signature,
//         callback: signatureResolve!
//     });
//     try{
//         //this await can only be resolved by the subscribeSignature callback; the timeout always rejects, never resolves
//         const signatureData: grpc.SignatureSubscriptionUpdate = await Promise.race([timeoutPromise, signaturePromise]);

//         // at this point, signature has been processed
//         clearTimeout(timeoutId!);

//         if(signatureData.slot == ""){
//             throw new Error("Slot not found");
//         }

//         if(signatureData.error){
//             throw signatureData.error;
//         }

//         logger.trace("Received transaction processed for signature "+signature+ " from gRPC");
//         if(commitment === "processed"){
//             return signatureData.trades;
//         }
//         else if(commitment === "confirmed"){
//             logger.trace("Waiting for slot confirmation for slot "+signatureData.slot);
//             await grpc.awaitSlotConfirmation(signatureData.slot);
//             return signatureData.trades;
//         }
//     }catch(error){
//         throw error;
//     }finally{
//         unsubscribe();
//     }
// }

// +++ INITIALIZATION +++

export async function init(){
    logger.info("Initializing...");
    await Promise.all([grpc.init(), setRecentBlockhash()]);
    setRecentBlockhashLoop();
    logger.info("Initialization complete");
}

// +++ BLOCKHASH HANDLING +++
let recentBlockhash: Readonly<{
    blockhash: Blockhash;
    lastValidBlockHeight: bigint;
}>;

//get recent blockhash loop

async function setRecentBlockhashLoop(){
    while (true) {
        try{
            await setRecentBlockhash();
            await new Promise(resolve => setTimeout(resolve, 10000))
        } catch(error){
            logger.trace("Error getting latest blockhash", error);
            await new Promise(resolve => setTimeout(resolve, 3000));
        }
    }
}

// throws error if it fails, make sure to catch
async function setRecentBlockhash(){
    const { value: latestBlockhash } = await basicRpc.getLatestBlockhash().send();
    if(!latestBlockhash){
        throw new Error("Failed to get latest blockhash");
    }
    recentBlockhash = latestBlockhash;
}

    
// +++ FUNCTIONS +++

//gets trades from a transaction
export async function getTransactionTrades(signature: Signature): Promise<Trade[] | undefined>{
    const transaction = await performanceRpc.getTransaction(signature, {encoding: "json", maxSupportedTransactionVersion: 0}).send();
    return await web3jsTransactionToTrades(transaction as unknown as coreUtils.GetTransactionJsonResponseV2);
}


// creates an empty transaction with two instructions: setComputeUnitLimit and setComputeUnitPrice
// if jitoTipLamports is greater than 0, it will add a jito tip to the transaction
export async function createTransaction({feePayer, priorityFeeLamports, estimatedComputeUnits, jitoTipLamports, nozomiTipLamports}: CreateTransactionParams){
    try{
        const realEstimatedComputeUnits = jitoTipLamports > 0 ? estimatedComputeUnits + 450 : estimatedComputeUnits + 300;
        const setComputeUnitLimitInstruction = getSetComputeUnitLimitInstruction({
            units: realEstimatedComputeUnits * 2
        });
        const priorityFeeMicroLamports = Math.floor((priorityFeeLamports * 500000) / realEstimatedComputeUnits); // we multiply by 500000 because the priority fee is calculated using the setComputeUnitLimit CU
        const setComputeUnitPriceInstruction = getSetComputeUnitPriceInstruction({
            microLamports: priorityFeeMicroLamports
        });

        const transactionWithLifetime = pipe(
            createTransactionMessage({ version: 0 }),
            tx => setTransactionMessageFeePayer(feePayer.address, tx),
            tx => setTransactionMessageLifetimeUsingBlockhash(recentBlockhash, tx),
            tx => appendTransactionMessageInstruction(setComputeUnitLimitInstruction, tx),
            tx => (priorityFeeLamports > 0 ? appendTransactionMessageInstruction(setComputeUnitPriceInstruction, tx) : tx)
        )

        if(nozomiTipLamports > 0){
            const randomNozomiTipAccount = constants.NOZOMI_TIP_ADDRESSES[Math.floor(Math.random() * constants.NOZOMI_TIP_ADDRESSES.length)];
            const transferInstruction = getTransferSolInstruction({
                amount: nozomiTipLamports,
                destination: randomNozomiTipAccount,
                source: feePayer
            });
            return appendTransactionMessageInstruction(transferInstruction, transactionWithLifetime);
        }
        if (jitoTipLamports > 0) {
            const randomJitoTipAccount = constants.JITO_TIP_ADDRESSES[Math.floor(Math.random() * constants.JITO_TIP_ADDRESSES.length)];
            const transferInstruction = getTransferSolInstruction({
                amount: jitoTipLamports,
                destination: randomJitoTipAccount,
                source: feePayer
            });
            return appendTransactionMessageInstruction(transferInstruction, transactionWithLifetime);
        }
        return transactionWithLifetime;
    }catch(error){
        logger.error("Error creating transaction", error);
        throw error;
    }
}

// +++ TYPES +++

type CreateTransactionParams = {
    feePayer: KeyPairSigner;
    priorityFeeLamports: number;
    estimatedComputeUnits: number;
    jitoTipLamports: number;
    nozomiTipLamports: number;
}