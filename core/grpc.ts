import ClientPkg, { SubscribeRequest, SubscribeUpdate, SubscribeUpdateSlot, SubscribeUpdateTransaction, txErrDecode } from "@triton-one/yellowstone-grpc";
const Client = (ClientPkg as any).default || ClientPkg;
import { TransactionErrorSolana } from "@triton-one/yellowstone-grpc/dist/types.js";
import { ClientDuplexStream } from "@grpc/grpc-js";
import { Commitment } from "@solana/web3.js";
import * as config from '../config.js';
import { createLogger } from "../logger.js";
import { Type } from "typescript";
import { Trade } from "./tradeTypes.js";
import { grpcExistsMigration, grpcTransactionToTrades, tradeToPrice } from "./coreUtils.js";
import { RaydiumAddresses } from "./apis/raydiumApi.js";
import bs58 from "bs58";
import { fileURLToPath } from 'url';

const SLOT_CONFIRMATION_TIMEOUT = 60000;
const TRANSACTION_PROCESSING_TIMEOUT = 60000;
const CACHE_RETENTION_MS = 60000;


const logger = createLogger(fileURLToPath(import.meta.url));

const client = new Client(config.grpc_url, undefined, {});
let stream: ClientDuplexStream<SubscribeRequest, SubscribeUpdate>;

let cachedWalletClient = new Client(config.grpc_url, undefined, {});
let cachedWalletStream: ClientDuplexStream<SubscribeRequest, SubscribeUpdate>;

let subscribers: Subscriber<any, any>[] = [];
let signatureSubscribers: Subscriber<SignatureSubscriptionData, SignatureSubscriptionUpdate>[] = [];
let slotConfirmSubscribers: SlotConfirmSubscriber[] = [];

let awaitedAddresses: string[] = [];

// +++ INITIALIZATION & SETUP +++

//init function: needs to be awaited before running
export async function init(){
    try{
        try{
            stream.end();
            cachedWalletStream.end();
        }catch(error){
            //
        }
        stream = await client.subscribe();
        cachedWalletStream = await cachedWalletClient.subscribe();
        setupStreamEventHandlers(stream, cachedWalletStream);
        console.log("gRPC stream initialized");

        let request: SubscribeRequest = {
            accounts: {},
            slots: {
                slots: {}
            },
            transactions: {
                txs: { //raydium & pump transactions
                    vote: false,
                    accountInclude: ["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"],
                    //accountInclude: [],
                    accountExclude: [],
                    accountRequired: []
                }
            },
            transactionsStatus: {},
            entry: {},
            blocks: {},
            blocksMeta: {},
            accountsDataSlice: [],
        }

        let cachedRequest: SubscribeRequest = {
            accounts: {},
            slots: {
                slots: {}
            },
            transactions: {
                txs: { //cached wallets
                    vote: false,
                    accountInclude: config.cached_wallets,
                    //accountInclude: [],
                    accountExclude: [],
                    accountRequired: []
                }
            },
            transactionsStatus: {},
            entry: {},
            blocks: {},
            blocksMeta: {},
            accountsDataSlice: [],
        }

        return new Promise<void>((resolve, reject) => {
            try{
                stream.write(request, (err: any) => {
                    if (err === null || err === undefined) {
                        console.log("gRPC stream request sent");
                        resolve();
                    } else {
                    console.error("Failed to send gRPC stream request", err);
                    setTimeout(() => {
                        init();
                        }, 10000);
                    }
                });
                cachedWalletStream.write(cachedRequest, (err: any) => {
                    if (err === null || err === undefined) {
                        console.log("gRPC cached wallet stream request sent");
                    } else {
                        console.error("Failed to send gRPC cached wallet stream request", err);
                    }
                });
            }catch(error){
                logger.error("Error sending gRPC stream request", error);
            }
        });
    }catch(error){
        console.error("Failed to connect gRPC stream, retrying in 10 seconds...", error);
        await new Promise(resolve => setTimeout(resolve, 10000));
        await init();
    }
}
// let messageCount = 0;
// let lastLog = Date.now();
// let bytesReceived = 0;
//sets up the event handlers for the gRPC stream
function setupStreamEventHandlers(stream: ClientDuplexStream<SubscribeRequest, SubscribeUpdate>, cachedWalletStream: ClientDuplexStream<SubscribeRequest, SubscribeUpdate>){
    stream.on("data", async (data: SubscribeUpdate) => {
        // messageCount++;
        // bytesReceived += JSON.stringify(data).length; // approximate size

        // // Log stats every second
        // const now = Date.now();
        // if (now - lastLog >= 1000) {
        //     console.log({
        //         messagesPerSecond: messageCount,
        //         mbPerSecond: (bytesReceived / 1024 / 1024).toFixed(2),
        //         avgMessageSize: (bytesReceived / messageCount).toFixed(2),
        //         time: data.createdAt?.toTimeString()
        //     });
        //     messageCount = 0;
        //     bytesReceived = 0;
        //     lastLog = now;
        // }

        if(data.filters.includes("slots") && data.slot){
            handleSlotUpdate(data.slot);
            return;
        }

        if(data.filters.includes("txs")){
            handleTransactionUpdate(data);
        }
    });

    cachedWalletStream.on("data", async (data: SubscribeUpdate) => {
        if(data.filters.includes("txs")){
            handleCachedTransactionUpdate(data);
        }
    });

    stream.on("error", (err: any) => {
        console.error("Error in gRPC stream", err);
    });

    stream.on("end", () => {
        console.error("gRPC stream ended, attempting to reconnect...");
        setTimeout(() => {
            init();
        }, 500);
    });

    stream.on("close", () => {
        console.error("gRPC stream closed, attempting to reconnect...");
        setTimeout(() => {
            init();
        }, 500);
    });

    cachedWalletStream.on("error", (err: any) => {
        console.error("Error in gRPC cached wallet stream", err);
    });

    cachedWalletStream.on("end", () => {
        console.error("gRPC cached wallet stream ended, attempting to reconnect...");
        setTimeout(() => {
            init();
        }, 500);
    });

    cachedWalletStream.on("close", () => {
        console.error("gRPC cached wallet stream closed, attempting to reconnect...");
        setTimeout(() => {
            init();
        }, 500);
    });
}

async function handleTransactionUpdate(data: SubscribeUpdate){
    const transaction = data.transaction?.transaction;
    if(!transaction) return;
    const accountKeys = transaction.transaction?.message?.accountKeys.concat((transaction.meta?.loadedWritableAddresses ? transaction.meta?.loadedWritableAddresses : [])).concat((transaction.meta?.loadedReadonlyAddresses ? transaction.meta?.loadedReadonlyAddresses : []));
    if(!accountKeys) return;

    // if(containsCachedWallet(accountKeys)){
    //     handleCachedTransactionUpdate(data);
    //     return;
    // }

    //pre-check if the transaction is relevant
    //if(!areAccountKeysAwaiting(accountKeys)) return;

    const waiters = getWaiters(accountKeys);

    for(const subscriber of waiters){
        const preprocessedData = await subscriber.form.preprocess(data, subscriber.data);
        if(preprocessedData != undefined){
            subscriber.callback(preprocessedData);
        }
    }
}

async function handleCachedTransactionUpdate(data: SubscribeUpdate){
    const transaction = data.transaction?.transaction;
    if(!transaction) return;

    const slot = data.transaction?.slot;
    const signature = bs58.encode(transaction.signature);
    if(!slot || !data.transaction) return;

    for(const subscriber of signatureSubscribers){
        if(subscriber.data.signature === signature){
            const preprocessedData = await subscriber.form.preprocess(data, subscriber.data);
            if(preprocessedData != undefined){
                subscriber.callback(preprocessedData);
            }
            return;
        }
    }

    signatureCache.set(signature, {
        slot,
        timestamp: Date.now(),
        update: data.transaction
    });
}

function refreshSubscriberMaps() {
    // Clear existing maps
    subscribersByWallet.clear();
    subscribersByMint.clear();
    
    // Rebuild maps from subscribers array
    for (const subscriber of subscribers) {
        if (subscriber.form.name === "wallet") {
            const wallet = subscriber.data.wallet;
            const existing = subscribersByWallet.get(wallet) || [];
            existing.push(subscriber);
            subscribersByWallet.set(wallet, existing);
        } else if (subscriber.form.name === "coin") {
            const mint = subscriber.data.mint;
            const existing = subscribersByMint.get(mint) || [];
            existing.push(subscriber);
            subscribersByMint.set(mint, existing);
        }
    }
}

// function areAccountKeysAwaiting(accountKeys: Uint8Array[]){
//     for(const address of accountKeys){
//         if(awaitedAddresses.includes(bs58.encode(address))){
//             return true;
//         }
//     }
//     return false;
// }

// Add these data structures near other subscriber declarations
let subscribersByWallet = new Map<string, Subscriber<any, any>[]>();
let subscribersByMint = new Map<string, Subscriber<any, any>[]>();

function getWaiters(accountKeys: Uint8Array[]) {
    const waiters = [];
    
    // For each account key, do direct lookups in both maps
    for (const key of accountKeys) {
        const address = bs58.encode(key);
        
        const walletSubs = subscribersByWallet.get(address);
        if (walletSubs) {
            waiters.push(...walletSubs);
        }
        
        const mintSubs = subscribersByMint.get(address);
        if (mintSubs) {
            waiters.push(...mintSubs);
        }
    }
    
    return waiters;
}
// +++ EXTERNAL FUNCTIONS +++
export async function confirmSignature({signature}: {signature: string}): Promise<Trade[] | undefined> {
    let timeoutId: NodeJS.Timeout;
    let signatureResolve: (data: SignatureSubscriptionUpdate) => void;

    //timeoutPromise is always rejected, never resolves
    const timeoutPromise = new Promise<SignatureSubscriptionUpdate>((resolve, reject) => {
        timeoutId = setTimeout(() => {
            logger.warn("Signature confirmation timeout for signature "+signature);
            reject(new Error("Signature confirmation timeout"));
        }, TRANSACTION_PROCESSING_TIMEOUT);
    });

    //signaturePromise is resolved by the subscribeSignature callback;
    const signaturePromise = new Promise<SignatureSubscriptionUpdate>((resolve, reject) => {
        signatureResolve = resolve;
    });
    
    const unsubscribe = subscribeSignature({
        signature,
        callback: signatureResolve!
    });
    try{
        const cachedTrades = await getTradesFromCachedSignature(signature);
        if(cachedTrades){
            return cachedTrades;
        }
        //this await can only be resolved by the subscribeSignature callback; the timeout always rejects, never resolves
        const signatureData: SignatureSubscriptionUpdate = await Promise.race([timeoutPromise, signaturePromise]);

        if(signatureData.slot == ""){
            throw new Error("Slot not found");
        }

        if(signatureData.error){
            throw signatureData.error;
        }

        logger.trace("Received transaction processed for signature "+signature+ " from gRPC");
        logger.trace("Waiting for slot confirmation for slot "+signatureData.slot);
        await awaitSlotConfirmation(signatureData.slot);
        return signatureData.trades;
        
    }catch(error){
        throw error;
    }finally{
        clearTimeout(timeoutId!);
        unsubscribe();
    }
}

// +++ SUBSCRIPTION FUNCTIONS (not slot confirmations)+++

/*
All subscription functions return an unsubscribe function.
*/

export async function subscribeTimedCoin({mint, delayMs, callback}: {mint: string, delayMs: number, callback: (data: CoinSubscriptionUpdate) => any}): Promise<() => void> {
    let price: number;
    let platform: "raydium" | "pump";

    const internalCallback = async (data: CoinSubscriptionUpdate) => {
        if(data.type === "price"){
            price = data.tokensPerLamport;
            platform = data.platform;
        }else if(data.type === "migration"){
            callback(data);
        }
    }

    const subscriber: Subscriber<CoinSubscriptionData, CoinSubscriptionUpdate> = {
        id: createId(),
        form: coinSubscriptionForm,
        data: {
            mint
        },
        callback: internalCallback
    }
    subscribers.push(subscriber);
    refreshSubscriberMaps();
    logger.trace("Subscribed to timed coin "+mint+ " with id "+subscriber.id);

    const intervalId = setInterval(() => {
        if(price !== undefined){
            const data: CoinSubscriptionUpdate = {type: "price", tokensPerLamport: price, platform: platform};
            callback(data);
        }else{
            const data: CoinSubscriptionUpdate = {type: "null"};
            callback(data);
        }
    }, delayMs);

    const internalUnsubscribe = () => {
        clearInterval(intervalId!);
        unsubscribe(subscriber.id);
    }

    return internalUnsubscribe;
}

export function unsubscribe(id: string){
    logger.trace("Unsibscribing id", id);
    subscribers = subscribers.filter(subscriber => subscriber.id !== id);
    refreshSubscriberMaps();
}

export function unsubscribeSignature(id: string){
    logger.trace("Unsubscribing signature id", id);
    signatureSubscribers = signatureSubscribers.filter(subscriber => subscriber.id !== id);
}

export function subscribeSignature({signature, callback}: {signature: string, callback: (data: SignatureSubscriptionUpdate) => any}): () => void {
    const subscriber: Subscriber<SignatureSubscriptionData, SignatureSubscriptionUpdate> = {
        id: createId(),
        form: signatureSubscriptionForm,
        data: {
            signature
        },
        callback: callback
    }
    signatureSubscribers.push(subscriber);
    logger.trace("Subscribed to signature "+signature+ " with id "+subscriber.id);
    const internalUnsubscribe = () => {   
        unsubscribeSignature(subscriber.id);
    }
    return internalUnsubscribe;
}

export async function subscribeCoin({mint, callback}: {mint: string, callback: (data: CoinSubscriptionUpdate) => any}): Promise<() => void> {
    const subscriber: Subscriber<CoinSubscriptionData, CoinSubscriptionUpdate> = {
        id: createId(),
        form: coinSubscriptionForm,
        data: {
            mint
        },
        callback: callback
    }
    subscribers.push(subscriber);
    refreshSubscriberMaps();
    logger.trace("Subscribed to coin "+mint+ " with id "+subscriber.id);
    const internalUnsubscribe = () => {   
        unsubscribe(subscriber.id);
    }
    return internalUnsubscribe;
}

export async function subscribeWallet({wallet, callback}: {wallet: string, callback: (data: WalletSubscriptionUpdate) => any}): Promise<() => void> {
    const subscriber: Subscriber<WalletSubscriptionData, WalletSubscriptionUpdate> = {
        id: createId(),
        form: walletSubscriptionForm,
        data: {wallet},
        callback: callback
    }
    subscribers.push(subscriber);
    refreshSubscriberMaps();
    logger.trace("Subscribed to wallet "+wallet+ " with id "+subscriber.id);
    const internalUnsubscribe = () => {   
        unsubscribe(subscriber.id);
    }
    return internalUnsubscribe;
}

// +++ INTERNAL FUNCTIONS +++

/*
This function is used to await the confirmation of a slot. It returns a promise that resolves when the slot is confirmed or rejects if the slot is rejected or the timeout is reached.
*/
export async function awaitSlotConfirmation(slot: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        slotConfirmSubscribers.push({slot, confirmationCallback: () => {
            logger.trace("Slot "+slot+" confirmed");
            resolve();
        }, rejectionCallback: () => {
            logger.trace("Slot "+slot+" rejected");
            reject(new Error("Slot "+slot+" rejected"));
        }});
        setTimeout(() => {
            slotConfirmSubscribers = slotConfirmSubscribers.filter(subscriber => subscriber.slot !== slot);
            reject(new Error("Slot "+slot+" confirmation timeout"));
        }, SLOT_CONFIRMATION_TIMEOUT);
    });
}


// +++ SUBSCRIPTION FORMS +++

/*
Subscriptions work as a facade between the subscriber and the gRPC stream.
We will define different forms of subscriptions, each with a different preprocess function, request type and update type.
-The preprocess function is used to check whether the update is relevant for the subscriber and transform the data into a format that the callback function can use. If it returns undefined, the callback is not called.
-The data type is the type of the data that the subscriber passes to subscribe.
-The update type is the type of update that the subscriber callback function receives.
*/
type SubscriptionForm<SubscriptionDataType, SubscriptionUpdateType> = {
    name: string;
    preprocess: (update: SubscribeUpdate, data: SubscriptionDataType) => Promise<SubscriptionUpdateType | undefined>;
}


/*
SIGNATURE SUBSCRIPTION
Subscribe to all trades for a given signature.
*/

export type SignatureSubscriptionData = {
    signature: string;
}

export type SignatureSubscriptionUpdate = {
    slot: string;
    trades: Trade[] | undefined;
    error: TransactionErrorSolana | undefined;
};

const signatureSubscriptionForm: SubscriptionForm<SignatureSubscriptionData, SignatureSubscriptionUpdate> = {
    name: "signature",
    preprocess: async (update: SubscribeUpdate, data: SignatureSubscriptionData) => {
        if(update.transaction && update.transaction.transaction?.signature && bs58.encode(update.transaction.transaction?.signature) === data.signature){
            if(update.transaction.transaction?.meta?.err?.err){
                return {
                    slot: update.transaction.slot,
                    trades: undefined,
                    error: txErrDecode.decode(update.transaction.transaction.meta.err.err)
                };
            }
            const trades = await grpcTransactionToTrades(update.transaction);
            if(trades){
                return {
                    slot: update.transaction.slot,
                    trades: trades,
                    error: undefined
                };
            }
            return {
                slot: update.transaction.slot,
                trades: undefined,
                error: undefined
            };
        }
        return undefined;
    }
}

/*
COIN SUBSCRIPTION
Subscribe to all coin updates for a given mint.
*/

type CoinSubscriptionData = {
    mint: string;
}

/*
The update can be a migration or a price update.
*/
export type MigrationUpdate = {
    type: "migration";
    raydiumAddresses: RaydiumAddresses;
}

export type PriceUpdate = {
    type: "price";
    platform: "raydium" | "pump";
    tokensPerLamport: number;
}

//for updating when no price is yet available in timed coin subscriptions
export type NullUpdate = {
    type: "null";
}

export type CoinSubscriptionUpdate = MigrationUpdate | PriceUpdate | NullUpdate;

const coinSubscriptionForm: SubscriptionForm<CoinSubscriptionData, CoinSubscriptionUpdate> = {
    name: "coin",
    preprocess: async (update: SubscribeUpdate, data: CoinSubscriptionData) => {
        if(update.transaction){
            const accountKeyStrings = (update.transaction?.transaction?.transaction?.message?.accountKeys ? update.transaction.transaction.transaction.message.accountKeys : []).map(key => bs58.encode(key));
            if(!(accountKeyStrings.includes(data.mint))){
                return undefined;
            }
            const trades = (await grpcTransactionToTrades(update.transaction))?.filter(trade => trade.mint === data.mint);
            if(trades && trades.length > 0){
                const price = await tradeToPrice(trades[0]);
                if(price){
                    return {
                        type: "price",
                        platform: trades[0].platform,
                        tokensPerLamport: price
                    }
                }
            }
            const migration = await grpcExistsMigration(update.transaction);
            if(migration){
                return {
                    type: "migration",
                    raydiumAddresses: migration
                }
            }
        }
        return undefined;
    }
}

/*
WALLET SUBSCRIPTION
Subscribe to all trades for a given wallet. Useful for copy trading.
*/

type WalletSubscriptionData = {
    wallet: string;
}

export type WalletSubscriptionUpdate = {
    trades: Trade[];
}

const walletSubscriptionForm: SubscriptionForm<WalletSubscriptionData, WalletSubscriptionUpdate> = {
    name: "wallet",
    preprocess: async (update: SubscribeUpdate, data: WalletSubscriptionData) => {
        if(update.transaction){
            const accountKeyStrings = (update.transaction?.transaction?.transaction?.message?.accountKeys ? update.transaction.transaction.transaction.message.accountKeys : []).map(key => bs58.encode(key));
            if(!(accountKeyStrings.includes(data.wallet))){
                return undefined;
            }
            const trades = (await grpcTransactionToTrades(update.transaction))?.filter(trade => trade.wallet === data.wallet);
            if(trades && trades.length > 0){
                return {
                    trades: trades
                }
            }
        }
        return undefined;
    }
}

/*
Every subscriber has an id.
*/
type Subscriber<SubscriptionDataType, SubscriptionUpdateType> = {
    id: string;
    form: SubscriptionForm<SubscriptionDataType, SubscriptionUpdateType>;
    data: SubscriptionDataType;
    callback: (data: SubscriptionUpdateType) => any;
}

function createId() : string {
    return crypto.randomUUID().replace(/-/g, '');
}


// +++ SLOT CONFIRMATION HANDLING +++

/*
Subscribers who subscribe to slot confirmations are treated differently from the other subscribers: 
we always listen to the slot confirmations stream and pass the data to the callback functions of the listeners.
There is no need to send a subscribe request for slot confirmations, we just need to listen to the stream.
*/
type SlotConfirmSubscriber = {
    slot: string;
    confirmationCallback: () => any;
    rejectionCallback: () => any;
}

function handleSlotUpdate(data: SubscribeUpdateSlot){
    const slot = data.slot;

    /*
    Slot statuses:
        0: "processed/frozen"
        1: "confirmed/optimisticConfirmation"
        2: "rooted/finalized"
        3: "firstShredReceived"
        4: "completed"
        5: "createdBank"
        6: "dead"
    */
    const status = data.status;
    if (status === 0) {
        slotCache.set(slot, { status: 'processed', timestamp: Date.now() });
    } else if(status === 1 || status === 2){
        slotCache.set(slot, { status: 'confirmed', timestamp: Date.now() });
        const waitingSubscribers = slotConfirmSubscribers.filter(subscriber => subscriber.slot === slot);
        for(const subscriber of waitingSubscribers){
            subscriber.confirmationCallback();
        }
        slotConfirmSubscribers = slotConfirmSubscribers.filter(subscriber => subscriber.slot !== slot);
    }
    else if (status === 6){
        slotCache.set(slot, { status: 'dead', timestamp: Date.now() });
        const waitingSubscribers = slotConfirmSubscribers.filter(subscriber => subscriber.slot === slot);
        for(const subscriber of waitingSubscribers){
            subscriber.rejectionCallback();
        }
        slotConfirmSubscribers = slotConfirmSubscribers.filter(subscriber => subscriber.slot !== slot);
    }
}

// slot & signature caching

// Cache structures
type SignatureCache = {
    slot: string;
    timestamp: number;
    update: SubscribeUpdateTransaction;
};

type SlotCache = {
    status: 'processed' | 'confirmed' | 'dead';
    timestamp: number;
};

// Cache maps with 60 second retention
const signatureCache = new Map<string, SignatureCache>();
const slotCache = new Map<string, SlotCache>();

// Cleanup function that runs periodically
function cleanupCaches() {
    const now = Date.now();
    for (const [signature, data] of signatureCache.entries()) {
        if (now - data.timestamp > CACHE_RETENTION_MS) {
            signatureCache.delete(signature);
        }
    }
    for (const [slot, data] of slotCache.entries()) {
        if (now - data.timestamp > CACHE_RETENTION_MS) {
            slotCache.delete(slot);
        }
    }
}

// Set up periodic cleanup
setInterval(cleanupCaches, CACHE_RETENTION_MS / 2);

// function to check if a signature is confirmed from cache
export async function getTradesFromCachedSignature(signature: string): Promise<Trade[] | undefined> {
    const sigData = signatureCache.get(signature);
    if (!sigData) return undefined; // Not in cache
    
    const slotData = slotCache.get(sigData.slot);
    if (!slotData) return undefined; // Slot not in cache
    
    if (slotData.status === 'dead' || slotData.status === 'processed') return undefined;
    if(sigData.update.transaction?.meta?.err?.err){
        throw txErrDecode.decode(sigData.update.transaction.meta.err.err);
    }
    return await grpcTransactionToTrades(sigData.update);
}