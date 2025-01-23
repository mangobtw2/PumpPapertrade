import { AccountRole, KeyPairSigner, addSignersToTransactionMessage, getSignatureFromTransaction, signTransactionMessageWithSigners, appendTransactionMessageInstruction, appendTransactionMessageInstructions, getStructCodec, getU8Codec, getU64Codec, address } from "@solana/web3.js";
import { Address } from "@solana/addresses";
import * as coreUtils from "../coreUtils.js";
import * as core from "../core.js";
import * as path from 'path';
import * as fs from 'fs';
import * as constants from '../constants.js';
import {basicRpc, performanceRpc} from "../rpc.js";
import bs58 from 'bs58';
import { getRaydiumAddressesFromMarketId } from "../coreUtils.js";

import { createLogger } from "../../logger.js";
import { Trade } from "../tradeTypes.js";

import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const logger = createLogger(import.meta.url);

// swap functions

/*
    returns true if the transaction was sent and confirmed, false otherwise
*/
// export async function buyRaydium(fromKeypair: KeyPairSigner, mint: Address, lamports: number, priorityFeeLamports: number, jitoTipLamports: number) {
export async function buyRaydium({fromKeypair, mint, lamportsIn, minimumAmountOut, priorityFeeLamports, jitoTipLamports, nozomiTipLamports, raydiumAddresses}: BuyRaydiumParams): Promise<Trade | undefined> {
    try {
        let addresses = raydiumAddresses;
        if (!addresses) {
            addresses = await getRaydiumAddresses(mint);
        }
        if (!addresses) {
            logger.warn("Could not get raydium addresses for mint "+mint.toString());
            return;
        }
        const transaction = await core.createTransaction({feePayer: fromKeypair, priorityFeeLamports, estimatedComputeUnits: 50000, jitoTipLamports, nozomiTipLamports});
        const buyInstruction = await getRaydiumBuyInstruction({fromKeypair, addresses, lamportsIn, minimumAmountOut});
        const idempotentInstruction = await coreUtils.getCreateIdempotentInstruction({fromKeypair, mint});
        const transactionWithInstruction = appendTransactionMessageInstructions([idempotentInstruction, buyInstruction], transaction);
        let signedTransaction;
        try{
            signedTransaction = await signTransactionMessageWithSigners(addSignersToTransactionMessage([fromKeypair], transactionWithInstruction));
        }catch(error){
            logger.warn("Error signing transaction", error);
            throw error;
        }
    
        logger.debug("Buying "+lamportsIn+" lamports of "+mint.toString()+ ", tx signature "+getSignatureFromTransaction(signedTransaction));
        const trades = await (nozomiTipLamports > 0 ? core.sendAndConfirmPerformanceWithNozomi : (jitoTipLamports > 0 ? core.sendAndConfirmPerformanceWithJito : core.sendAndConfirmPerformance))(signedTransaction, {commitment: "confirmed", skipPreflight: true});
        if(!trades){
            logger.error("Error buying on Raydium: no trades returned by send and confirm, but no error thrown");
            return undefined;
        } else if(trades.length > 1){
            logger.error("Error buying on Raydium: multiple trades returned by send and confirm");
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
                const errorMessage = getRaydiumErrorFromCode(errorCode);
                if (errorMessage) {
                    logger.warn("Error buying on Raydium: "+errorMessage);
                    return;
                }
            } else if ('__code' in error.context){
                const errorCode = error.context.__code as number;
                const errorMessage = getRaydiumErrorFromCode(errorCode);
                if (errorMessage) {
                    logger.warn("Error buying on Raydium: "+errorMessage);
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
            const errorMessage = getRaydiumErrorFromCode(errorCode);
            if (errorMessage) {
                logger.warn("Error buying on Raydium: "+errorMessage);
                throw errorCode;
            }
        }
        logger.warn("Error creating, sending and confirming buy transaction:", error);
        throw error;
    }
}

export async function sellRaydium({fromKeypair, mint, tokenAmountIn, minimumAmountOut, priorityFeeLamports, jitoTipLamports, nozomiTipLamports, raydiumAddresses}: SellRaydiumParams): Promise<Trade | undefined> {
    try {
        let addresses = raydiumAddresses;
        if (!addresses) {
            addresses = await getRaydiumAddresses(mint);
        }
        if (!addresses) {
            logger.warn("Could not get raydium addresses for mint "+mint.toString());
            return;
        }
        const transaction = await core.createTransaction({feePayer: fromKeypair, priorityFeeLamports, estimatedComputeUnits: 30724, jitoTipLamports, nozomiTipLamports});
        const sellInstruction = await getRaydiumSellInstruction({fromKeypair, addresses, tokenAmountIn, minimumAmountOut});
        const transactionWithInstruction = appendTransactionMessageInstruction(sellInstruction, transaction);
        const signedTransaction = await signTransactionMessageWithSigners(addSignersToTransactionMessage([fromKeypair], transactionWithInstruction));;
    
        logger.debug("Selling "+tokenAmountIn+" tokens of "+mint.toString()+ ", tx signature "+getSignatureFromTransaction(signedTransaction));
        const trades = await (nozomiTipLamports > 0 ? core.sendAndConfirmPerformanceWithNozomi : (jitoTipLamports > 0 ? core.sendAndConfirmPerformanceWithJito : core.sendAndConfirmPerformance))(signedTransaction, {commitment: "confirmed", skipPreflight: true});
        if(!trades || trades.length === 0){
            logger.error("Error selling on Raydium: no trades returned by send and confirm, but no error thrown");
            return undefined;
        } else if(trades.length > 1){
            logger.error("Error selling on Raydium: multiple trades returned by send and confirm");
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
                const errorMessage = getRaydiumErrorFromCode(errorCode);
                if (errorMessage) {
                    logger.warn("Error selling on Raydium: "+errorMessage);
                    return;
                }
            } else if ('__code' in error.context){
                const errorCode = error.context.__code as number;
                const errorMessage = getRaydiumErrorFromCode(errorCode);
                if (errorMessage) {
                    logger.warn("Error selling on Raydium: "+errorMessage);
                    return;
                }
            }
        } else if(
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
            const errorMessage = getRaydiumErrorFromCode(errorCode);
            if (errorMessage) {
                logger.warn("Error selling on Raydium: "+errorMessage);
                throw errorCode;
            }
        }
        logger.warn("Error creating, sending and confirming sell transaction:", error);
        throw error;
    }
}

// instruction getters

async function getRaydiumSellInstruction({fromKeypair, addresses, tokenAmountIn, minimumAmountOut}: {fromKeypair: KeyPairSigner, addresses: RaydiumAddresses, tokenAmountIn: bigint, minimumAmountOut: bigint}){
    try{
        const wsolAccount = await coreUtils.getAssociatedTokenAddress({owner: fromKeypair.address, mint: constants.WSOL_ADDRESS});
        const tokenAccount = await coreUtils.getAssociatedTokenAddress({owner: fromKeypair.address, mint: addresses.mint});
        return {
            programAddress: constants.RAYDIUM_PROGRAM_ADDRESS,
            accounts: [
                {
                    address: constants.TOKEN_PROGRAM_ADDRESS,
                    role: AccountRole.READONLY
                },
                {
                    address: addresses.amm,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RAYDIUM_AUTHORITY_ADDRESS,
                    role: AccountRole.READONLY
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: addresses.pool1,
                    role: AccountRole.WRITABLE
                },
                {
                    address: addresses.pool2,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: tokenAccount,
                    role: AccountRole.WRITABLE
                },
                {
                    address: wsolAccount,
                    role: AccountRole.WRITABLE
                },{
                    address: fromKeypair.address,
                    role: AccountRole.WRITABLE_SIGNER
                }

            ],
            data: raydiumSwapCodec.encode({
                discriminator: 9,
                amountIn: tokenAmountIn,
                minimumAmountOut: minimumAmountOut
            }) as Uint8Array
        }
    }catch(error){
        logger.error("Error creating sell instruction", error);
        throw error;
    }
}


async function getRaydiumBuyInstruction({fromKeypair, addresses, lamportsIn, minimumAmountOut}: {fromKeypair: KeyPairSigner, addresses: RaydiumAddresses, lamportsIn: bigint, minimumAmountOut: bigint}){
    try{
        const wsolAccount = await coreUtils.getAssociatedTokenAddress({owner: fromKeypair.address, mint: constants.WSOL_ADDRESS});
        const tokenAccount = await coreUtils.getAssociatedTokenAddress({owner: fromKeypair.address, mint: addresses.mint});
        return {
            programAddress: constants.RAYDIUM_PROGRAM_ADDRESS,
            accounts: [
                {
                    address: constants.TOKEN_PROGRAM_ADDRESS,
                    role: AccountRole.READONLY
                },
                {
                    address: addresses.amm,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RAYDIUM_AUTHORITY_ADDRESS,
                    role: AccountRole.READONLY
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: addresses.pool1,
                    role: AccountRole.WRITABLE
                },
                {
                    address: addresses.pool2,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: constants.RANDOM_ADDRESS,
                    role: AccountRole.WRITABLE
                },
                {
                    address: wsolAccount,
                    role: AccountRole.WRITABLE
                },
                {
                    address: tokenAccount,
                    role: AccountRole.WRITABLE
                },{
                    address: fromKeypair.address,
                    role: AccountRole.WRITABLE_SIGNER
                }

            ],
            data: raydiumSwapCodec.encode({
                discriminator: 9,
                amountIn: lamportsIn,
                minimumAmountOut: minimumAmountOut
            }) as Uint8Array
        }
    }catch(error){
        logger.error("Error creating buy instruction", error);
        throw error;
    }
    
}

// address cache

//const DATA_DIR = path.join(__dirname, 'data');
const CACHE_FILE_PATH = path.join(__dirname, 'raydium-cache.json');

const raydiumSwapCodec = getStructCodec([
    ["discriminator", getU8Codec()],
    ["amountIn", getU64Codec()],
    ["minimumAmountOut", getU64Codec()],
]);

let raydiumCache: RaydiumCache = loadRaydiumCache();

function loadRaydiumCache(): RaydiumCache {
    try {
        if (fs.existsSync(CACHE_FILE_PATH)) {
            const data = fs.readFileSync(CACHE_FILE_PATH, 'utf8');
            return JSON.parse(data);
        }
    } catch (error) {
        logger.warn('Failed to load Raydium cache:', error);
    }
    return {};
}

export function cacheRaydiumAddresses(addresses: RaydiumAddresses) {
    raydiumCache[addresses.mint.toString()] = addresses;
    try {
        fs.writeFileSync(CACHE_FILE_PATH, JSON.stringify(raydiumCache, null, 2));
    } catch (error) {
        logger.error('Failed to save Raydium cache:', error);
    }
}

export async function getRaydiumAddresses(mint: Address): Promise<RaydiumAddresses | undefined> {
    if (raydiumCache[mint.toString()]) {
        return raydiumCache[mint.toString()];
    }
    const marketIdFromApi = await getMarketIdFromApi(mint);
    if(marketIdFromApi){
        const raydiumAddresses = await getRaydiumAddressesFromMarketId({mint, marketId: marketIdFromApi});
        if(raydiumAddresses){
            cacheRaydiumAddresses(raydiumAddresses);
            return raydiumAddresses;
        }
    }
    logger.debug("Could not get market id from api, trying chain using GPA");
    const marketIdFromChain = await getMarketIdFromChain(mint);
    if(marketIdFromChain){
        const raydiumAddresses = await getRaydiumAddressesFromMarketId({mint, marketId: marketIdFromChain});
        if(raydiumAddresses){
            cacheRaydiumAddresses(raydiumAddresses);
            return raydiumAddresses;
        }
    }
    return undefined;
}

export async function getMarketIdFromApi(mint: Address): Promise<Address | undefined> {
    try{
        const response = await fetch(`https://api-v3.raydium.io/pools/info/mint?mint1=${constants.WSOL_ADDRESS.toString()}&mint2=${mint.toString()}&poolType=all&poolSortField=default&sortType=desc&pageSize=1&page=1`, {
            method: 'GET'
        });
        const data = await response.json();
        if(!data.data.data || data.data.data.length === 0){
            return undefined;
        }
        return address(data.data.data[0].marketId);
    }catch(error){
        logger.debug("Error getting amm id from api", error);
        return undefined
    }
}

export async function getMarketIdFromChain(mint: Address): Promise<Address | undefined> {
    try{
        const accounts = await performanceRpc.getProgramAccounts(constants.OPENBOOK_PROGRAM_ADDRESS, {
            filters: [
                { dataSize: 388n },
                {
                    memcmp: {
                        offset: 85n,
                        bytes: Buffer.from(bs58.decode(mint.toString())).toString('base64'),
                        encoding: "base64"
                    },
                },
                {
                    memcmp: {
                        offset: 53n,
                        bytes: Buffer.from(bs58.decode(constants.WSOL_ADDRESS.toString())).toString('base64'),
                        encoding: "base64"
                    },
                }
            ],
            encoding: 'base64'
        }).send();
    
        if(!accounts[0].account) {
            logger.debug("No openbook program accounts found while GPA for mint "+mint.toString());
            return undefined;
        } 
        if(accounts.length > 1){
            logger.debug("Multiple openbook program accounts found while GPA for mint "+mint.toString());
            return undefined;
        }
        return accounts[0].pubkey;
    }catch(error){
        logger.warn("Error getting market id from chain", error);
        return undefined;
    }
}


function getRaydiumErrorFromCode(code: number){
    if (code == 1){
        return "timed out: blockhash expired"
    } else if (code === 30){
        return "slippage exceeded"
    } else if (code === 38) {
        return "possibly forgot to wrap sol?";
    } else if (code === 40){
        return "insufficient funds";
    }
    return undefined;
}


// types

export type RaydiumAddresses = {
    mint: Address;
    pool1: Address;
    pool2: Address;
    amm: Address;
}

type RaydiumCache = {
    [mintAddress: string]: RaydiumAddresses;
}

export type BuyRaydiumParams = {fromKeypair: KeyPairSigner, 
    mint: Address, 
    lamportsIn: bigint, 
    minimumAmountOut: bigint,
    priorityFeeLamports: number, 
    jitoTipLamports: number,
    nozomiTipLamports: number,
    raydiumAddresses: RaydiumAddresses | undefined
}

export type SellRaydiumParams = {fromKeypair: KeyPairSigner, 
    mint: Address, 
    tokenAmountIn: bigint, 
    minimumAmountOut: bigint,
    priorityFeeLamports: number, 
    jitoTipLamports: number,
    nozomiTipLamports: number,
    raydiumAddresses: RaydiumAddresses | undefined
}