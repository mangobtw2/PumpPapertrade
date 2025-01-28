import ClientPkg, { SubscribeRequest, SubscribeUpdate, SubscribeUpdateSlot, SubscribeUpdateTransaction, txErrDecode } from "@triton-one/yellowstone-grpc";
const Client = (ClientPkg as any).default || ClientPkg;
import { TransactionErrorSolana } from "@triton-one/yellowstone-grpc/dist/types.js";
import { ClientDuplexStream } from "@grpc/grpc-js";
import { Commitment } from "@solana/web3.js";
import * as config from './config.js';
import { createLogger } from "./logger.js";
import { Type } from "typescript";
import { Trade } from "./tradeTypes.js";
import { grpcExistsMigration, grpcTransactionToBondingCurvePoolBalances, grpcTransactionToPoolBalances, grpcTransactionToTrades, tradeToPrice } from "./coreUtils.js";
import bs58 from "bs58";
import { fileURLToPath } from 'url';
import fs, { StatsListener } from "fs";
import { createClient } from 'redis';
import * as pumpFunCalc from "./pumpFunCalc.js";
import * as raydiumCalc from "./raydiumCalc.js";


const logger = createLogger(fileURLToPath(import.meta.url));

const client = new Client(config.grpc_url, undefined, {});
let pumpStream: ClientDuplexStream<SubscribeRequest, SubscribeUpdate>;
let raydiumStream: ClientDuplexStream<SubscribeRequest, SubscribeUpdate>;

type BondingCurvePoolBalance = {
    mint: string;
    virtualSolReserves: bigint;
    virtualTokenReserves: bigint;
}

type PoolBalance = {
    ammId: string;
    solPool: bigint;
    tokenPool: bigint;
}

type TokenInfo = {
    migrated: boolean;
    bondingCurvePoolBalance: BondingCurvePoolBalance | undefined;
}

let tokenInfoMap = new Map<string, TokenInfo>();

let mintToAmmIdMap = new Map<string, string>();
let poolBalancesByAmmId = new Map<string, {solPool: bigint, tokenPool: bigint}>();

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
            pumpStream.end();
            raydiumStream.end();
        }catch(error){
            //
        }
        pumpStream = await client.subscribe();
        raydiumStream = await client.subscribe();
        setupPumpStreamEventHandlers(pumpStream);
        setupRaydiumStreamEventHandlers(raydiumStream);
        console.log("gRPC stream initialized");

        let pumpRequest: SubscribeRequest = {
            accounts: {},
            slots: {
                slots: {}
            },
            transactions: {
                txs: { //pump transactions
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

        let raydiumRequest: SubscribeRequest = {
            accounts: {},
            slots: {
                slots: {}
            },
            transactions: {
                txs: { //pump transactions
                    vote: false,
                    accountInclude: ["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"],
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
                pumpStream.write(pumpRequest, (err: any) => {
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
            try{
                raydiumStream.write(raydiumRequest, (err: any) => {
                    if (err === null || err === undefined){
                        console.log("gRPC stream request sent");
                        resolve();
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
function setupPumpStreamEventHandlers(stream: ClientDuplexStream<SubscribeRequest, SubscribeUpdate>){
    stream.on("data", async (data: SubscribeUpdate) => {

        const now = Date.now();
        if (now - lastLog >= 1000) {
            console.log({
                time: data.createdAt?.toTimeString()
            });
            lastLog = now;
        }

        handlePumpTransactionUpdate(data);
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

function setupRaydiumStreamEventHandlers(stream: ClientDuplexStream<SubscribeRequest, SubscribeUpdate>){
    stream.on("data", async (data: SubscribeUpdate) => {

        const now = Date.now();
        if (now - lastLog >= 1000) {
            console.log({
                time: data.createdAt?.toTimeString()
            });
            lastLog = now;
        }

        handleRaydiumTransactionUpdate(data);
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
    tokensBought: bigint;
    waitingForTimestamp: number; //waiting for timestamp
    waitingForSell: number; //waiting for sell 1, 2, 3 or 4 (1 means waiting for migration) (0 for migration timeout), -1 (waiting for buy)
}

async function handlePumpTransactionUpdate(data: SubscribeUpdate){
    try{
        if(!data.transaction) return;
        const trades = await grpcTransactionToTrades(data.transaction);
        const bondingCurvePoolBalance = await grpcTransactionToBondingCurvePoolBalances(data.transaction);
        if(bondingCurvePoolBalance){
            const poolBalance = bondingCurvePoolBalance[0];
            tokenInfoMap.set(poolBalance.mint, {
                migrated: false,
                bondingCurvePoolBalance: {
                    mint: poolBalance.mint,
                    virtualSolReserves: poolBalance.virtualSolReserves,
                    virtualTokenReserves: poolBalance.virtualTokenReserves
                }
            });
        }
        if(trades){
            for(const trade of trades){
                if(trade.direction == "buy" && trade.lamports >= 40000000n){
                    trackBuy(trade);
                }
            }
        }
    }catch(e){}
    
}

async function handleRaydiumTransactionUpdate(data: SubscribeUpdate){
    //TODO: implement
    try{
        if(!data.transaction) return;
        const existsMigration = await grpcExistsMigration(data.transaction);
        if(existsMigration){
            mintToAmmIdMap.set(existsMigration.mint, existsMigration.amm);
            poolBalancesByAmmId.set(existsMigration.amm, {
                solPool: 79005300000n,
                tokenPool: 206900000000000n
            });
            const existingTokenInfo = tokenInfoMap.get(existsMigration.mint);
            if(!existingTokenInfo){
                tokenInfoMap.set(existsMigration.mint, {
                    migrated: true,
                    bondingCurvePoolBalance: undefined
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
        }else{
            //update pool balances
            const poolBalances = await grpcTransactionToPoolBalances(data.transaction);
            if(poolBalances){
                const poolBalance = poolBalances[0];
                poolBalancesByAmmId.set(poolBalance.ammId, {
                    solPool: poolBalance.solPool,
                    tokenPool: poolBalance.tokenPool
                });
            }
        }
    }catch(error){
        console.error("Error in gRPC stream", error);
    }
}

function createID(){
    return crypto.randomUUID();
}

async function trackBuy(trade: Trade){
    const buyStatus: Status = {
        positionID: createID(),
        mint: trade.mint,
        address: trade.wallet,
        tokensBought: 0n,
        waitingForTimestamp: Date.now() + 1000 * 1.5,
        waitingForSell: -1
    }
    queue.push(buyStatus);
}

async function sellQuarter(status: Status){
    if(status.waitingForSell == -1){
        const bondingCurvePoolBalance = tokenInfoMap.get(status.mint)?.bondingCurvePoolBalance;
        if(!bondingCurvePoolBalance) return;
        const buyAmount = pumpFunCalc.getBuyOutAmount(1000000000n, bondingCurvePoolBalance.virtualSolReserves, bondingCurvePoolBalance.virtualTokenReserves);
        const id = createID();
        const migrationTimeoutStatus: Status = {
            positionID: id,
            mint: status.mint,
            address: status.address,
            tokensBought: buyAmount,
            waitingForTimestamp: Date.now() + 1000 * 60 * 60 * 12,
            waitingForSell: 0
        }
        queue.push(migrationTimeoutStatus);
        const migrationSellStatus: Status = {
            positionID: id,
            mint: status.mint,
            address: status.address,
            tokensBought: buyAmount,
            waitingForTimestamp: 0,
            waitingForSell: 1
        }
        const existingMigrationList = waitingForMigrationMap.get(status.mint);
        if(existingMigrationList){
            existingMigrationList.push(migrationSellStatus);
        }else{
            waitingForMigrationMap.set(status.mint, [migrationSellStatus]);
        }
        
        // Use Redis List to append trade data for the wallet
        try{
            await redisClient.lPush(`tradesPump:${status.address}`, JSON.stringify({
                positionID: status.positionID,  // same ID to connect with buy
                amount: -1,  // negative for buy
                timestamp: Date.now(),
                mint: status.mint
            }));
        }catch(error){
            console.error("Failed to append trade data to Redis", error);
        }
        return;
    }
    if(status.waitingForSell == 0 && !tokenInfoMap.get(status.mint)?.migrated) {
        //remove from waitingForMigrationMap as we timed out
        const existingMigrationList = waitingForMigrationMap.get(status.mint);
        if(existingMigrationList){
            waitingForMigrationMap.set(status.mint, existingMigrationList.filter(item => item.positionID != status.positionID));
        }
        const currentBondingCurvePoolBalance = tokenInfoMap.get(status.mint)?.bondingCurvePoolBalance;
        if(!currentBondingCurvePoolBalance) return;
        const sellAmount = Number(pumpFunCalc.getSellOutAmount(status.tokensBought, currentBondingCurvePoolBalance.virtualSolReserves, currentBondingCurvePoolBalance.virtualTokenReserves)) / 10 ** 9;
        try{
            await redisClient.lPush(`tradesPump:${status.address}`, JSON.stringify({
                positionID: status.positionID,  // same ID to connect with buy
                amount: sellAmount,  // positive for sell
                timestamp: Date.now(),
                mint: status.mint
            }));
        }catch(error){
            console.error("Failed to append trade data to Redis", error);
        }
        return;
    };
    if(status.waitingForSell == 1){
        const sellAmount = Number(raydiumCalc.getOutAmount(206900000000000n, 79005359123n, status.tokensBought / 4n)) / 10 ** 9;
        // Append sell trade to the wallet's trade list
        try{
            await redisClient.lPush(`tradesPump:${status.address}`, JSON.stringify({
                positionID: status.positionID,  // same ID to connect with buy
                amount: sellAmount,  // positive for sell
                timestamp: Date.now(),
                mint: status.mint
            }));
        }catch(error){
            console.error("Failed to append trade data to Redis", error);
        }
            queue.push({
                ...status,
                waitingForTimestamp: Date.now() + 1000 * 150,
                waitingForSell: status.waitingForSell + 1
            });
    }else{
        const ammId = mintToAmmIdMap.get(status.mint);
        if(!ammId) return;
        const currentPoolBalance = poolBalancesByAmmId.get(ammId);
        if(!currentPoolBalance) return;
        const sellAmount = Number(raydiumCalc.getOutAmount(currentPoolBalance.tokenPool, currentPoolBalance.solPool, status.tokensBought / 4n)) / 10 ** 9;
        
        // Append sell trade to the wallet's trade list
        try{
            await redisClient.lPush(`tradesPump:${status.address}`, JSON.stringify({
                positionID: status.positionID,  // same ID to connect with buy
                amount: sellAmount,  // positive for sell
                timestamp: Date.now(),
                mint: status.mint
            }));
        }catch(error){
            console.error("Failed to append trade data to Redis", error);
        }

        if(status.waitingForSell < 4) {
            queue.push({
                ...status,
                waitingForTimestamp: status.waitingForTimestamp + 1000 * 150,
                waitingForSell: status.waitingForSell + 1
            });
        }
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