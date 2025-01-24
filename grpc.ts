import ClientPkg, { SubscribeRequest, SubscribeUpdate, SubscribeUpdateSlot, SubscribeUpdateTransaction, txErrDecode } from "@triton-one/yellowstone-grpc";
const Client = (ClientPkg as any).default || ClientPkg;
import { TransactionErrorSolana } from "@triton-one/yellowstone-grpc/dist/types.js";
import { ClientDuplexStream } from "@grpc/grpc-js";
import { Commitment } from "@solana/web3.js";
import * as config from './config.js';
import { createLogger } from "./logger.js";
import { Type } from "typescript";
import { Trade } from "./tradeTypes.js";
import { grpcExistsMigration, grpcTransactionToTrades, tradeToPrice } from "./coreUtils.js";
import bs58 from "bs58";
import { fileURLToPath } from 'url';
import fs, { StatsListener } from "fs";
import { createClient } from 'redis';


const logger = createLogger(fileURLToPath(import.meta.url));

const client = new Client(config.grpc_url, undefined, {});
let stream: ClientDuplexStream<SubscribeRequest, SubscribeUpdate>;

type TokenInfo = {
    migrated: boolean;
    tokensPerLamport: number;
}

let tokenInfoMap = new Map<string, TokenInfo>();

const redisClient = createClient({
    url: 'redis://localhost:6379',
    socket: {
        reconnectStrategy: retries => Math.min(retries * 50, 1000)
    }
});

// +++ INITIALIZATION & SETUP +++

//init function: needs to be awaited before running
export async function init(clearMemory: boolean = false){
    try{
        await redisClient.connect();
    }catch(error){
        console.error("Failed to connect to Redis", error);
    }
    
    if (clearMemory) {
        await clearRedisMemory();
    }
    
    try{
        try{
            stream.end();
        }catch(error){
            //
        }
        stream = await client.subscribe();
        setupStreamEventHandlers(stream);
        console.log("gRPC stream initialized");

        let request: SubscribeRequest = {
            accounts: {},
            slots: {
                slots: {}
            },
            transactions: {
                txs: { //raydium & pump transactions
                    vote: false,
                    accountInclude: ["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"],
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
let lastLog = Date.now();
//sets up the event handlers for the gRPC stream
function setupStreamEventHandlers(stream: ClientDuplexStream<SubscribeRequest, SubscribeUpdate>){
    stream.on("data", async (data: SubscribeUpdate) => {

        const now = Date.now();
        if (now - lastLog >= 1000) {
            console.log({
                time: data.createdAt?.toTimeString()
            });
            lastLog = now;
        }

        handleTransactionUpdate(data);
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
}

let queue: Status[] = [];

let waitingForMigrationMap = new Map<string, Status[]>();

setInterval(() => {
    const now = Date.now();
    // Find expired items
    const expiredItems = queue.filter(item => item.waitingForTimestamp <= now);
    // Process expired items
    for (const item of expiredItems) {
        sellQuarter(item);
    }
    // Remove expired items from queue
    queue = queue.filter(item => item.waitingForTimestamp > now);
}, 1000);

type Status = {
    positionID: string;
    mint: string;
    address: string;
    initialTokensPerLamport: number;
    waitingForTimestamp: number; //waiting for timestamp
    waitingForSell: number; //waiting for sell 1, 2, 3 or 4 (1 means waiting for migration) (0 for migration timeout)
}

async function handleTransactionUpdate(data: SubscribeUpdate){
    if(!data.transaction) return;
    const existsMigration = await grpcExistsMigration(data.transaction);
    if(existsMigration){
        const existingTokenInfo = tokenInfoMap.get(existsMigration.mint);
        if(!existingTokenInfo){
            tokenInfoMap.set(existsMigration.mint, {
                migrated: true,
                tokensPerLamport: 0
            });
        }else{
            existingTokenInfo.migrated = true;
        }
        const existingMigrationList = waitingForMigrationMap.get(existsMigration.mint);
        if(existingMigrationList){
            for(const item of existingMigrationList){
                sellQuarter(item);
            }
        }
        return;
    }
    const trades = await grpcTransactionToTrades(data.transaction);
    if(trades){
        for(const trade of trades){
            const tokensPerLamport = Number(trade.tokens) / Number(trade.lamports);
            const existingTokenInfo = tokenInfoMap.get(trade.mint);
            if(!existingTokenInfo){
                tokenInfoMap.set(trade.mint, {
                    migrated: false,
                    tokensPerLamport: tokensPerLamport
                });
            }
            else{
                existingTokenInfo.tokensPerLamport = tokensPerLamport;
            }
            if(trade.direction == "buy"){
                trackBuy(trade, tokensPerLamport);
            }
        }
    }
}

function createID(){
    return crypto.randomUUID();
}

async function trackBuy(trade: Trade, tokensPerLamport: number){
    const migrationTimeoutStatus: Status = {
        positionID: createID(),
        mint: trade.mint,
        address: trade.wallet,
        initialTokensPerLamport: tokensPerLamport,
        waitingForTimestamp: Date.now() + 1000 * 60 * 60 * 12,
        waitingForSell: 0
    }
    queue.push(migrationTimeoutStatus);
    const status: Status = {
        positionID: createID(),
        mint: trade.mint,
        address: trade.wallet,
        initialTokensPerLamport: tokensPerLamport,
        waitingForTimestamp: 0,
        waitingForSell: 1
    }
    const existingMigrationList = waitingForMigrationMap.get(trade.mint);
    if(existingMigrationList){
        existingMigrationList.push(status);
    }else{
        waitingForMigrationMap.set(trade.mint, [status]);
    }
    
    // Use Redis List to append trade data for the wallet
    await redisClient.lPush(`tradesPump:${trade.wallet}`, JSON.stringify({
        positionID: status.positionID,  // same ID to connect with buy
        amount: -1,  // negative for buy
        timestamp: Date.now(),
        mint: status.mint
    }));
}

async function sellQuarter(status: Status){
    if(status.waitingForSell == 0 && !tokenInfoMap.get(status.mint)?.migrated) {
        //remove from waitingForMigrationMap as we timed out
        const existingMigrationList = waitingForMigrationMap.get(status.mint);
        if(existingMigrationList){
            waitingForMigrationMap.set(status.mint, existingMigrationList.filter(item => item.positionID != status.positionID));
        }
        const currentTokensPerLamport = tokenInfoMap.get(status.mint)?.tokensPerLamport;
        if(!currentTokensPerLamport) return;
        const sellAmount = status.initialTokensPerLamport / currentTokensPerLamport;
        await redisClient.lPush(`tradesPump:${status.address}`, JSON.stringify({
            positionID: status.positionID,  // same ID to connect with buy
            amount: sellAmount,  // positive for sell
            timestamp: Date.now(),
            mint: status.mint
        }));
        return;
    };
    const currentTokensPerLamport = tokenInfoMap.get(status.mint)?.tokensPerLamport;
    if(!currentTokensPerLamport) return;
    const sellAmount = (status.initialTokensPerLamport / currentTokensPerLamport) / 4;
    
    // Append sell trade to the wallet's trade list
    await redisClient.lPush(`tradesPump:${status.address}`, JSON.stringify({
        positionID: status.positionID,  // same ID to connect with buy
        amount: sellAmount,  // positive for sell
        timestamp: Date.now(),
        mint: status.mint
    }));

    if(status.waitingForSell < 4) {
        queue.push({
            ...status,
            waitingForTimestamp: status.waitingForTimestamp + 1000 * 150,
            waitingForSell: status.waitingForSell + 1
        });
    }
}

// Add these functions for memory management
export async function clearRedisMemory() {
    try {
        // Get all keys matching the pattern "tradesPump:*"
        const keys = await redisClient.keys('tradesPump:*');
        
        if (keys.length > 0) {
            // Delete all matched keys
            await redisClient.del(keys);
            logger.info(`Redis memory cleared successfully for ${keys.length} tradesPump keys`);
        } else {
            logger.info('No tradesPump keys found to clear');
        }
    } catch (error) {
        logger.error('Error clearing Redis memory:', error);
    }
}

// Optional: Add a function to get memory usage
export async function getRedisMemoryInfo() {
    const info = await redisClient.info('memory');
    return info;
}