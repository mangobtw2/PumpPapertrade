import * as constants from './constants.js';
import bs58 from 'bs58';
import { getProgramDerivedAddress, Address, address, KeyPairSigner, AccountRole, getU64Codec, getU8Codec, getStructCodec, getAddressCodec, getU128Codec, getI64Codec, getBooleanCodec, GetTransactionApi, Base58EncodedBytes, TokenAmount } from '@solana/web3.js';
import { Trade } from './tradeTypes.js';
import { SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo } from "@triton-one/yellowstone-grpc";
import { getTransferSolInstructionDataCodec} from '@solana-program/system'
import { getTransferInstructionDataCodec} from '@solana-program/token'
import { CompiledInstruction, InnerInstructions, TokenBalance } from '@triton-one/yellowstone-grpc/dist/grpc/solana-storage.js';
import {createLogger} from './logger.js';
import { RaydiumAddresses } from './core/apis/raydiumApi.js';

const logger = createLogger(import.meta.url);

// +++ ADDRESS GETTERS +++

export async function getAssociatedTokenAddress({owner, mint}: {owner: Address, mint: Address}){
    const associatedTokenAddress = await getProgramDerivedAddress({
        programAddress: constants.ASSOCIATED_TOKEN_PROGRAM_ADDRESS,
        seeds: [Buffer.from(bs58.decode(String(owner))), Buffer.from(bs58.decode(String(constants.TOKEN_PROGRAM_ADDRESS))), Buffer.from(bs58.decode(String(mint)))],
    });
    return associatedTokenAddress[0];
}


// +++ RAYDIUM ADDRESS GETTERS +++
export async function getRaydiumAddressesFromMarketId({mint, marketId}: {mint: Address, marketId: Address}){
    const pool1 = await getProgramDerivedAddress({
        programAddress: constants.RAYDIUM_PROGRAM_ADDRESS,
        seeds: [Buffer.from(bs58.decode(String(constants.RAYDIUM_PROGRAM_ADDRESS))), Buffer.from(bs58.decode(String(marketId))), Buffer.from('coin_vault_associated_seed')],
    });
    const pool2 = await getProgramDerivedAddress({
        programAddress: constants.RAYDIUM_PROGRAM_ADDRESS,
        seeds: [Buffer.from(bs58.decode(String(constants.RAYDIUM_PROGRAM_ADDRESS))), Buffer.from(bs58.decode(String(marketId))), Buffer.from('pc_vault_associated_seed')],
    });
    const ammId = await getProgramDerivedAddress({
        programAddress: constants.RAYDIUM_PROGRAM_ADDRESS,
        seeds: [Buffer.from(bs58.decode(String(constants.RAYDIUM_PROGRAM_ADDRESS))), Buffer.from(bs58.decode(String(marketId))), Buffer.from('amm_associated_seed')],
    });
    return {
        mint: mint,
        pool1: address(pool1[0]),
        pool2: address(pool2[0]),
        amm: address(ammId[0])
    };
}

// +++ PUMP.FUN ADDRESS GETTERS +++

export async function getBondingCurveAddress({mintPubkey}: {mintPubkey: Address}) {
    const bondingCurveAddress = await getProgramDerivedAddress({
        programAddress: constants.PUMP_FUN_PROGRAM_ADDRESS,
        seeds: [
            Buffer.from("bonding-curve"),
            Buffer.from(bs58.decode(String(mintPubkey)))
        ],
    });
    return bondingCurveAddress[0];
}

export async function getAssociatedBondingCurveAddress({mintPubkey, bondingCurve}: {mintPubkey: Address, bondingCurve: Address}) {
    const associatedBondingCurveAddress = await getProgramDerivedAddress({
        programAddress: constants.ASSOCIATED_TOKEN_PROGRAM_ADDRESS,
        seeds: [
            Buffer.from(bs58.decode(String(bondingCurve))),
            Buffer.from(bs58.decode(String(constants.TOKEN_PROGRAM_ADDRESS))),
            Buffer.from(bs58.decode(String(mintPubkey)))
        ],
    });
    return associatedBondingCurveAddress[0];
}


// +++ INSTRUCTION GETTERS +++

export async function getCreateIdempotentInstruction({fromKeypair, mint}: {fromKeypair: KeyPairSigner, mint: Address}){
    try{
        const tokenAccount = await getAssociatedTokenAddress({owner: fromKeypair.address, mint});
        return {
            programAddress: constants.ASSOCIATED_TOKEN_PROGRAM_ADDRESS,
            accounts: [
                {
                    address: fromKeypair.address,
                    role: AccountRole.WRITABLE_SIGNER
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
                    address: mint,
                    role: AccountRole.READONLY
                },
                {
                    address: constants.SYSTEM_PROGRAM_ADDRESS,
                    role: AccountRole.READONLY
                },
                {
                    address: constants.TOKEN_PROGRAM_ADDRESS,
                    role: AccountRole.READONLY
                }
                
            ],
            data: new Uint8Array([1])
        }
    }catch(error){
        logger.error("Error creating idempotent instruction", error);
        throw error;
    }
}

//returns the price in tokens per lamport
export async function tradeToPrice(trade: Trade): Promise<number>{
    return Number(trade.tokens) / Number(trade.lamports);
}

// +++ WEB3.JS TYPE CONVERSIONS +++

const transferCodec = getTransferInstructionDataCodec();
const transferSolCodec = getTransferSolInstructionDataCodec();
const pumpLogCodec = getStructCodec([
    ["discriminator1", getU128Codec()],
    ["mint", getAddressCodec()],
    ["solAmount", getU64Codec()],
    ["tokenAmount", getU64Codec()],
    ["isBuy", getBooleanCodec()],
    ["user", getAddressCodec()],
    ["timestamp", getI64Codec()],
    ["virtualSolReserves", getU64Codec()],
    ["virtualTokenReserves", getU64Codec()],
    ["discriminator2", getU128Codec()],
]);

export interface GetTransactionJsonResponseV2 {
    blockTime?: number;
    meta?: {
      computeUnitsConsumed?: number;
      err: any;
      fee?: number;
      innerInstructions?: Array<{
        index: number;
        instructions: Array<{
          accounts: number[];
          data: string;
          programIdIndex: number;
          stackHeight?: number;
        }>;
      }>;
      loadedAddresses?: {
        readonly: string[];
        writable: string[];
      };
      logMessages?: string[];
      postBalances?: number[];
      postTokenBalances?: Array<{
        accountIndex: number;
        mint: string;
        owner: string;
        programId: string;
        uiTokenAmount: {
          amount: string;
          decimals: number;
          uiAmount: number | null;
          uiAmountString: string;
        };
      }>;
      preBalances?: number[];
      preTokenBalances?: Array<{
        accountIndex: number;
        mint: string;
        owner: string;
        programId: string;
        uiTokenAmount: {
          amount: string;
          decimals: number;
          uiAmount: number | null;
          uiAmountString: string;
        };
      }>;
      rewards?: any[];
      status?: {
        Ok: null | any;
        Err?: any;
      };
    };
    slot?: number;
    transaction?: {
      message: {
        accountKeys: string[];
        addressTableLookups?: Array<{
          accountKey: string;
          readonlyIndexes: number[];
          writableIndexes: number[];
        }>;
        header?: {
          numReadonlySignedAccounts: number;
          numReadonlyUnsignedAccounts: number;
          numRequiredSignatures: number;
        };
        instructions: Array<{
          accounts: number[];
          data: string;
          programIdIndex: number;
          stackHeight: number | null;
        }>;
        recentBlockhash: string;
      };
      signatures: string[];
    };
    version?: number;
  }

export async function web3jsTransactionToTrades(transaction: GetTransactionJsonResponseV2): Promise<Trade[] | undefined>{
    if(!transaction) return undefined;
    
    const accountKeys = transaction.transaction?.message?.accountKeys.concat(transaction.meta?.loadedAddresses?.writable ?? []).concat(transaction.meta?.loadedAddresses?.readonly ?? []);
    const instructions = transaction.transaction?.message?.instructions;
    const signature = transaction.transaction?.signatures[0];
    const innerInstructionGroups = transaction.meta?.innerInstructions;
    const block = transaction.slot;
    const timestamp = transaction.blockTime;
    if(!transaction.meta?.preTokenBalances || !transaction.meta?.postTokenBalances) return undefined;
    const allBalances = transaction.meta?.preTokenBalances?.concat(transaction.meta?.postTokenBalances);
    if(!accountKeys || !instructions || !innerInstructionGroups || !allBalances || !block || !timestamp || !signature) return undefined;

    let trades: Trade[] = [];
    //traversing over accounts and finding out whether raydium or pump.fun is involved
    for(let accountIndex = 0; accountIndex < accountKeys.length; accountIndex++){
        const accountKey = accountKeys[accountIndex];
        if(accountKey == constants.RAYDIUM_PROGRAM_ADDRESS){
            // RAYDIUM TRADE PARSING, we now know the account with accountIndex is the raydium program
            const raydiumTrades = await web3jsTransactionToRaydiumTrades({signature, accountKeys, instructions, innerInstructionGroups, allBalances, raydiumAccountIndex: accountIndex, block, timestamp});
            trades = trades.concat(raydiumTrades);
        }else if(accountKey == constants.PUMP_FUN_PROGRAM_ADDRESS){
            // PUMP.FUN TRADE PARSING, we now know the account with accountIndex is the pump.fun program
            const pumpTrades = await web3jsTransactionToPumpFunTrades({signature, accountKeys, instructions, innerInstructionGroups, allBalances, pumpAccountIndex: accountIndex, block, timestamp});
            trades = trades.concat(pumpTrades);
        }
    }
    if(trades.length == 0) return undefined;
    // fee calculation
    const feeLamportsString = transaction.meta?.fee;
    const preBalances = transaction.meta?.preBalances;
    const postBalances = transaction.meta?.postBalances;
    if(feeLamportsString == undefined || preBalances == undefined || postBalances == undefined){
        return trades;
    }
    let transactionFeeLamports = BigInt(feeLamportsString);
    
    const feeLamports = await web3jsCalculateFeeLamportsNew({accountKeys, preBalances, postBalances, transactionFeeLamports});

    const dividedFeeLamports = feeLamports / BigInt(trades.length);

    for(const trade of trades){
        trade.feeLamports = dividedFeeLamports;
    }
    return trades;
}

async function web3jsCalculateFeeLamportsNew({accountKeys, preBalances, postBalances, transactionFeeLamports}: {accountKeys: string[], preBalances: number[], postBalances: number[], transactionFeeLamports: bigint}): Promise<bigint>{
    let feeLamports = transactionFeeLamports;
    for(let i = 0; i < accountKeys.length; i++){
        const accountKey = accountKeys[i];
        if(constants.FEE_PAYING_ADDRESSES.includes(address(accountKey))){
            feeLamports += BigInt(postBalances[i] - preBalances[i]);
        }
    }
    return feeLamports;
}

async function web3jsTransactionToPumpFunTrades({signature, accountKeys, instructions, innerInstructionGroups, allBalances, pumpAccountIndex, block, timestamp}: {signature: string, accountKeys: string[], instructions: Array<{
    accounts: number[];
    data: string;
    programIdIndex: number;
    stackHeight: number | null;
  }>, innerInstructionGroups: Array<{
    index: number;
    instructions: Array<{
      accounts: number[];
      data: string;
      programIdIndex: number;
      stackHeight?: number;
    }>;
  }>, allBalances: Array<{
    accountIndex: number;
    mint: string;
    owner: string;
    programId: string;
    uiTokenAmount: {
      amount: string;
      decimals: number;
      uiAmount: number | null;
      uiAmountString: string;
    };
  }>, pumpAccountIndex: number, block: number, timestamp: number}): Promise<Trade[]>{
    let trades: Trade[] = [];

    const pumpLogCodecLength = pumpLogCodec.fixedSize;

    for(const innerInstructionGroup of innerInstructionGroups){
        for(const innerInstruction of innerInstructionGroup.instructions){
            try{
                if(innerInstruction.programIdIndex == pumpAccountIndex && bs58.decode(innerInstruction.data).length == pumpLogCodecLength){
                    const decoded = pumpLogCodec.decode(bs58.decode(innerInstruction.data));
                    trades.push({
                        signature: signature,
                        wallet: address(decoded.user),
                        platform: "pump",
                        direction: decoded.isBuy ? "buy" : "sell",
                        mint: address(decoded.mint),
                        lamports: decoded.solAmount,
                        tokens: decoded.tokenAmount,
                        feeLamports: 0n,
                        block: Number(block),
                        timestamp: Number(timestamp),
                        amm: undefined,
                        pool1: undefined,
                        pool2: undefined
                    });
                }
            }catch(error){
                logger.error("Error parsing pump.fun anchor log instruction", error);
                continue;
            }
        }
    }

    return trades;
}

async function web3jsTransactionToRaydiumTrades({signature, accountKeys, instructions, innerInstructionGroups, allBalances, raydiumAccountIndex, block, timestamp}: {signature: string, accountKeys: string[], instructions: Array<{
    accounts: number[];
    data: string;
    programIdIndex: number;
    stackHeight: number | null;
  }>, innerInstructionGroups: Array<{
    index: number;
    instructions: Array<{
      accounts: number[];
      data: string;
      programIdIndex: number;
      stackHeight?: number;
    }>;
  }>, allBalances: Array<{
    accountIndex: number;
    mint: string;
    owner: string;
    programId: string;
    uiTokenAmount: {
      amount: string;
      decimals: number;
      uiAmount: number | null;
      uiAmountString: string;
    };
  }>, raydiumAccountIndex: number, block: number, timestamp: number}): Promise<Trade[]>{
    let trades: Trade[] = [];

    //index of the token program in the account keys (over which transfer instructions are done)
    const tokenProgramIndex = accountKeys.findIndex(account => account == constants.TOKEN_PROGRAM_ADDRESS);

    // first traversing over instructions to find trades
    for(let instructionIndex = 0; instructionIndex < instructions.length; instructionIndex++){
        const instruction = instructions[instructionIndex];
        try{
            if(instruction.programIdIndex == raydiumAccountIndex && bs58.decode(instruction.data)[0] == 9){
                //we now know that instruction is a raydium swap instruction
                const amm = accountKeys[instruction.accounts[1]];
                const pool1 = accountKeys[instruction.accounts[instruction.accounts.length == 18 ? 5 : 4]];
                const pool2 = accountKeys[instruction.accounts[instruction.accounts.length == 18 ? 6 : 5]];
                const wallet = accountKeys[instruction.accounts[instruction.accounts.length == 18 ? 17 : 16]];
                const sourceTokenAccount = accountKeys[instruction.accounts[instruction.accounts.length == 18 ? 15 : 14]];
                const destinationTokenAccount = accountKeys[instruction.accounts[instruction.accounts.length == 18 ? 16 : 15]];

                //getting the inner token account interactions (possibly transfer) instructions for the trade
                let innerTransferInstructions = innerInstructionGroups.find(innerInstructionGroup => innerInstructionGroup.index == instructionIndex)?.instructions.filter(instruction => instruction.programIdIndex == tokenProgramIndex);
                if(innerTransferInstructions == undefined) return [];
                //getting the source and destination token changes
                let sourceTokenChange;
                let destinationTokenChange;
                for(const innerTransferInstruction of innerTransferInstructions){
                    try{
                        const decoded = transferCodec.decode(bs58.decode(innerTransferInstruction.data));
                        if(decoded.discriminator == 3){
                            // we now know that this is a transfer instruction
                            if(accountKeys[innerTransferInstruction.accounts[0]] == sourceTokenAccount){
                                sourceTokenChange = decoded.amount;
                            }else if(accountKeys[innerTransferInstruction.accounts[1]] == destinationTokenAccount){
                                destinationTokenChange = decoded.amount;
                            }
                        }
                    }catch(error){
                        continue;
                    }
                }
                if(sourceTokenChange == undefined || destinationTokenChange == undefined) return [];

                const sourceNotWsol = allBalances.find(balance => balance.owner == wallet && balance.mint != constants.WSOL_ADDRESS && balance.accountIndex == instruction.accounts[instruction.accounts.length == 18 ? 15 : 14])?.mint;
                const destinationNotWsol = allBalances.find(balance => balance.owner == wallet && balance.mint != constants.WSOL_ADDRESS && balance.accountIndex == instruction.accounts[instruction.accounts.length == 18 ? 16 : 15])?.mint;
                if(sourceNotWsol && destinationNotWsol){
                    // both source and destination are non-wsol tokens, skip
                    continue;
                }
                //check if there exists a non-wsol token in the source or destination
                if(destinationNotWsol){
                    // buy
                    trades.push({
                        signature: signature,
                        wallet: address(wallet),
                        platform: "raydium",
                        direction: "buy",
                        mint: address(destinationNotWsol),
                        lamports: sourceTokenChange,
                        tokens: destinationTokenChange,
                        feeLamports: 0n,
                        block: Number(block),
                        timestamp: Number(timestamp),
                        amm: address(amm),
                        pool1: address(pool1),
                        pool2: address(pool2)
                    });
                }else if(sourceNotWsol){
                    // sell
                    trades.push({
                        signature: signature,
                        wallet: address(wallet),
                        platform: "raydium",
                        direction: "sell",
                        mint: address(sourceNotWsol),
                        lamports: destinationTokenChange,
                        tokens: sourceTokenChange,
                        feeLamports: 0n,
                        block: Number(block),
                        timestamp: Number(timestamp),
                        amm: address(amm),
                        pool1: address(pool1),
                        pool2: address(pool2)
                    });
                }else{
                    continue;
                }
                
            }
        }catch(error){
            logger.error("Error parsing raydium swap instruction", error);
            continue;
        }
    }
    //now traversing over inner instructions, possibly hidden swaps
    for(const innerInstructionGroup of innerInstructionGroups){
        for(const innerInstruction of innerInstructionGroup.instructions){
            try{
                if(innerInstruction.programIdIndex == raydiumAccountIndex && bs58.decode(innerInstruction.data)[0] == 9){
                    // we now know that this is a raydium swap instruction
                    const amm = accountKeys[innerInstruction.accounts[1]];
                    const pool1 = accountKeys[innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 5 : 4]];
                    const pool2 = accountKeys[innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 6 : 5]];
                    const wallet = accountKeys[innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 17 : 16]];
                    const sourceTokenAccount = accountKeys[innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 15 : 14]];
                    const destinationTokenAccount = accountKeys[innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 16 : 15]];

                    //getting the inner transfer instructions for the trade
                    let innerTransferInstructions = innerInstructionGroup.instructions.filter(instruction => 
                        instruction.programIdIndex == tokenProgramIndex && 
                        instruction.stackHeight && 
                        innerInstruction.stackHeight && 
                        instruction.stackHeight == innerInstruction.stackHeight + 1 
                    );
                    if(innerTransferInstructions == undefined) return [];
                    //getting the source and destination token changes
                    let sourceTokenChange;
                    let destinationTokenChange;
                    for(const innerTransferInstruction of innerTransferInstructions){
                        try{
                            const decoded = transferCodec.decode(bs58.decode(innerTransferInstruction.data));
                            if(decoded.discriminator == 3){
                                // we now know that this is a transfer instruction
                                if(accountKeys[innerTransferInstruction.accounts[0]] == sourceTokenAccount){
                                    if(sourceTokenChange != undefined) return [];
                                    sourceTokenChange = decoded.amount;
                                }else if(accountKeys[innerTransferInstruction.accounts[1]] == destinationTokenAccount){
                                    if(destinationTokenChange != undefined) return [];
                                    destinationTokenChange = decoded.amount;
                                }
                            }
                        }catch(error){
                            continue;
                        }
                        
                    }
                    if(sourceTokenChange == undefined || destinationTokenChange == undefined) return [];

                    const sourceNotWsol = allBalances.find(balance => balance.owner == wallet && balance.mint != constants.WSOL_ADDRESS && balance.accountIndex == innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 15 : 14])?.mint;
                    const destinationNotWsol = allBalances.find(balance => balance.owner == wallet && balance.mint != constants.WSOL_ADDRESS && balance.accountIndex == innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 16 : 15])?.mint;

                    if(sourceNotWsol && destinationNotWsol){
                        // both source and destination are non-wsol tokens, skip
                        continue;
                    }

                    //check if the source token account is a wsol account
                    if(destinationNotWsol){
                        // buy
                        trades.push({
                            signature: signature,
                            wallet: address(wallet),
                            platform: "raydium",
                            direction: "buy",
                            mint: address(destinationNotWsol),
                            lamports: sourceTokenChange,
                            tokens: destinationTokenChange,
                            feeLamports: 0n,
                            block: Number(block),
                            timestamp: Number(timestamp),
                            amm: address(amm),
                            pool1: address(pool1),
                            pool2: address(pool2)
                        });
                    }else if(sourceNotWsol){
                        // sell
                        trades.push({
                            signature: signature,
                            wallet: address(wallet),
                            platform: "raydium",
                            direction: "sell",
                            mint: address(sourceNotWsol),
                            lamports: destinationTokenChange,
                            tokens: sourceTokenChange,
                            feeLamports: 0n,
                            block: Number(block),
                            timestamp: Number(timestamp),
                            amm: address(amm),
                            pool1: address(pool1),
                            pool2: address(pool2)
                        });
                    }
                }
            }catch(error){
                logger.error("Error parsing raydium swap instruction", error);
                continue;
            }
        }
    }
    return trades;
} 

// +++ GRPC FUNCTIONS +++

export async function grpcExistsMigration(transactionUpdate: SubscribeUpdateTransaction): Promise<RaydiumAddresses | undefined>{
    const transaction = transactionUpdate.transaction;
    if(!transaction) return;
    const accountKeys = transaction.transaction?.message?.accountKeys.concat((transaction.meta?.loadedWritableAddresses ? transaction.meta?.loadedWritableAddresses : [])).concat((transaction.meta?.loadedReadonlyAddresses ? transaction.meta?.loadedReadonlyAddresses : []));
    const instructions = transaction.transaction?.message?.instructions;
    if(!accountKeys || !instructions) return;

    for(let accountIndex = 0; accountIndex < accountKeys.length; accountIndex++){
        const accountKey = accountKeys[accountIndex];
        if(Buffer.compare(accountKey, constants.RAYDIUM_PROGRAM_ADDRESS_BYTES) == 0){
            for(const instruction of instructions){
                if(instruction.programIdIndex == accountIndex && instruction.data[0] == 1 && address(bs58.encode(accountKeys[instruction.accounts[17]])) == constants.PUMP_FUN_MIGRATION_ADDRESS){
                    //we now know that this is a migration/lp initialization instruction
                    return {
                        mint: address(bs58.encode(accountKeys[instruction.accounts[9]])),
                        pool1: address(bs58.encode(accountKeys[instruction.accounts[10]])),
                        pool2: address(bs58.encode(accountKeys[instruction.accounts[11]])),
                        amm: address(bs58.encode(accountKeys[instruction.accounts[4]]))
                    };
                }
            }
        }
    }
    return;
}

type BondingCurvePoolBalance = {
    mint: string;
    virtualSolReserves: bigint;
    virtualTokenReserves: bigint;
}

export async function grpcTransactionToBondingCurvePoolBalances(transactionUpdate: SubscribeUpdateTransaction): Promise<BondingCurvePoolBalance[] | undefined>{
    const transaction = transactionUpdate.transaction;
    if(!transaction) return undefined;
    const signature = bs58.encode(transaction.signature);
    const accountKeys = transaction.transaction?.message?.accountKeys.concat((transaction.meta?.loadedWritableAddresses ? transaction.meta?.loadedWritableAddresses : [])).concat((transaction.meta?.loadedReadonlyAddresses ? transaction.meta?.loadedReadonlyAddresses : []));
    const instructions = transaction.transaction?.message?.instructions;
    const innerInstructionGroups = transaction.meta?.innerInstructions;
    const postTokenBalances = transaction.meta?.postTokenBalances;
    const block = Number(transactionUpdate.slot);
    if(!accountKeys || !instructions || !innerInstructionGroups || !postTokenBalances || !block) return undefined;

    let poolBalances: BondingCurvePoolBalance[] = [];

    const pumpLogCodecLength = pumpLogCodec.fixedSize;

    const pumpAccountIndex = accountKeys.findIndex(accountKey => Buffer.compare(accountKey, constants.PUMP_FUN_PROGRAM_ADDRESS_BYTES) == 0);
    if(pumpAccountIndex == -1) return undefined;

    for(const innerInstructionGroup of innerInstructionGroups){
        for(const innerInstruction of innerInstructionGroup.instructions){
            try{
                if(innerInstruction.programIdIndex == pumpAccountIndex && innerInstruction.data.length == pumpLogCodecLength){
                    const decoded = pumpLogCodec.decode(innerInstruction.data);
                    poolBalances.push({
                        mint: address(decoded.mint),
                        virtualSolReserves: BigInt(decoded.virtualSolReserves),
                        virtualTokenReserves: BigInt(decoded.virtualTokenReserves)
                    });
                }
            }catch(error){
                logger.error("Error parsing pump.fun anchor log instruction", error);
                continue;
            }
        }
    }
}


type PoolBalance = {
    ammId: string;
    solPool: bigint;
    tokenPool: bigint;
}
export async function grpcTransactionToPoolBalances(transactionUpdate: SubscribeUpdateTransaction): Promise<PoolBalance[] | undefined>{
    const transaction = transactionUpdate.transaction;
    if(!transaction) return undefined;
    const signature = bs58.encode(transaction.signature);
    const accountKeys = transaction.transaction?.message?.accountKeys.concat((transaction.meta?.loadedWritableAddresses ? transaction.meta?.loadedWritableAddresses : [])).concat((transaction.meta?.loadedReadonlyAddresses ? transaction.meta?.loadedReadonlyAddresses : []));
    const instructions = transaction.transaction?.message?.instructions;
    const innerInstructionGroups = transaction.meta?.innerInstructions;
    const postTokenBalances = transaction.meta?.postTokenBalances;
    const block = Number(transactionUpdate.slot);
    if(!accountKeys || !instructions || !innerInstructionGroups || !postTokenBalances || !block) return undefined;

    let poolBalances: PoolBalance[] = [];
    //traversing over accounts and finding out whether raydium or pump.fun is involved
    let raydiumAccountIndex = -1;
    for(let accountIndex = 0; accountIndex < accountKeys.length; accountIndex++){
        const accountKey = accountKeys[accountIndex];
        if(Buffer.compare(accountKey, constants.RAYDIUM_PROGRAM_ADDRESS_BYTES) == 0){
            raydiumAccountIndex = accountIndex;
        }
    }
    if(raydiumAccountIndex == -1) return undefined;
    //now we know that the raydium account is at raydiumAccountIndex. getting pool balances

    // first traversing over instructions to find trades
    for(let instructionIndex = 0; instructionIndex < instructions.length; instructionIndex++){
        const instruction = instructions[instructionIndex];
        try{
            if(instruction.programIdIndex == raydiumAccountIndex && instruction.data[0] == 9){
                //we now know that instruction is a raydium swap instruction
                const ammId = accountKeys[instruction.accounts[1]];

                const pool1balance = postTokenBalances.find(balance => balance.accountIndex == instruction.accounts[instruction.accounts.length == 18 ? 5 : 4]);
                const pool2balance = postTokenBalances.find(balance => balance.accountIndex == instruction.accounts[instruction.accounts.length == 18 ? 6 : 5]);
                if(!pool1balance || !pool2balance) continue;
                
                const isPool1Wsol = pool1balance.mint == String(constants.WSOL_ADDRESS);
                const isPool2Wsol = pool2balance.mint == String(constants.WSOL_ADDRESS);
                if(!pool1balance.uiTokenAmount || !pool2balance.uiTokenAmount) continue;
                if(isPool1Wsol && isPool2Wsol){
                    // both pool1 and pool2 are wsol, skip
                    continue;
                }
                if(!isPool1Wsol && !isPool2Wsol){
                    // both pool1 and pool2 are non-wsol, skip
                    continue;
                }
                //check if there exists a non-wsol token in the pool1 or pool2
                if(isPool2Wsol){
                    // pool2 is wsol, pool1 is non-wsol
                    poolBalances.push({
                        ammId: bs58.encode(ammId),
                        solPool: BigInt(pool2balance.uiTokenAmount.amount),
                        tokenPool: BigInt(pool1balance.uiTokenAmount.amount)
                    });
                }
                if(isPool1Wsol){
                    // pool1 is wsol, pool2 is non-wsol
                    poolBalances.push({
                        ammId: bs58.encode(ammId),
                        solPool: BigInt(pool1balance.uiTokenAmount.amount),
                        tokenPool: BigInt(pool2balance.uiTokenAmount.amount)
                    });
                }
                
            }
        }catch(error){
            logger.error("Error parsing raydium swap instruction", error);
            continue;
        }
    }
    //now traversing over inner instructions, possibly hidden swaps
    for(const innerInstructionGroup of innerInstructionGroups){
        for(const innerInstruction of innerInstructionGroup.instructions){
            try{
                if(innerInstruction.programIdIndex == raydiumAccountIndex && innerInstruction.data[0] == 9){
                    // we now know that this is a raydium swap instruction
                    const ammId = accountKeys[innerInstruction.accounts[1]];
                    
                    const pool1balance = postTokenBalances.find(balance => balance.accountIndex == innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 5 : 4]);
                    const pool2balance = postTokenBalances.find(balance => balance.accountIndex == innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 6 : 5]);
                    if(!pool1balance || !pool2balance) continue;
                    
                    const isPool1Wsol = pool1balance.mint == String(constants.WSOL_ADDRESS);
                    const isPool2Wsol = pool2balance.mint == String(constants.WSOL_ADDRESS);

                    if(!pool1balance.uiTokenAmount || !pool2balance.uiTokenAmount) continue;
                    if(isPool1Wsol && isPool2Wsol){
                        // both pool1 and pool2 are wsol, skip
                        continue;
                    }
                    if(!isPool1Wsol && !isPool2Wsol){
                        // both pool1 and pool2 are non-wsol, skip
                        continue;
                    }
                    if(isPool2Wsol){
                        // pool2 is wsol, pool1 is non-wsol
                        poolBalances.push({
                            ammId: bs58.encode(ammId),
                            solPool: BigInt(pool2balance.uiTokenAmount.amount),
                            tokenPool: BigInt(pool1balance.uiTokenAmount.amount)
                        });
                    }
                    if(isPool1Wsol){
                        // pool1 is wsol, pool2 is non-wsol
                        poolBalances.push({
                            ammId: bs58.encode(ammId),
                            solPool: BigInt(pool1balance.uiTokenAmount.amount),
                            tokenPool: BigInt(pool2balance.uiTokenAmount.amount)
                        });
                    }   
                }
            }catch(error){
                logger.error("Error parsing raydium swap instruction", error);
                continue;
            }
        }
    }

    if(poolBalances.length == 0) return undefined;
    return poolBalances;
}

export async function grpcTransactionToTrades(transactionUpdate: SubscribeUpdateTransaction): Promise<Trade[] | undefined> {
    const transaction = transactionUpdate.transaction;
    if(!transaction) return undefined;
    const signature = bs58.encode(transaction.signature);
    const accountKeys = transaction.transaction?.message?.accountKeys.concat((transaction.meta?.loadedWritableAddresses ? transaction.meta?.loadedWritableAddresses : [])).concat((transaction.meta?.loadedReadonlyAddresses ? transaction.meta?.loadedReadonlyAddresses : []));
    const instructions = transaction.transaction?.message?.instructions;
    const innerInstructionGroups = transaction.meta?.innerInstructions;
    const allBalances = transaction.meta?.preTokenBalances.concat(transaction.meta?.postTokenBalances);
    const block = Number(transactionUpdate.slot);
    if(!accountKeys || !instructions || !innerInstructionGroups || !allBalances || !block) return undefined;

    let trades: Trade[] = [];
    //traversing over accounts and finding out whether raydium or pump.fun is involved
    for(let accountIndex = 0; accountIndex < accountKeys.length; accountIndex++){
        const accountKey = accountKeys[accountIndex];
        if(Buffer.compare(accountKey, constants.RAYDIUM_PROGRAM_ADDRESS_BYTES) == 0){
            // RAYDIUM TRADE PARSING, we now know the account with accountIndex is the raydium program
            const raydiumTrades = await grpcTransactionToRaydiumTrades({signature, accountKeys, instructions, innerInstructionGroups, allBalances, raydiumAccountIndex: accountIndex, block});
            trades = trades.concat(raydiumTrades);
        }else if(Buffer.compare(accountKey, constants.PUMP_FUN_PROGRAM_ADDRESS_BYTES) == 0){
            // PUMP.FUN TRADE PARSING, we now know the account with accountIndex is the pump.fun program
            const pumpTrades = await grpcTransactionToPumpFunTrades({signature, accountKeys, instructions, innerInstructionGroups, allBalances, pumpAccountIndex: accountIndex, block});
            trades = trades.concat(pumpTrades);
        }
    }
    if(trades.length == 0) return undefined;
    // fee calculation
    const feeLamportsString = transaction.meta?.fee;
    const preBalances = transaction.meta?.preBalances;
    const postBalances = transaction.meta?.postBalances;
    if(feeLamportsString == undefined || preBalances == undefined || postBalances == undefined) return undefined;
    let transactionFeeLamports = BigInt(feeLamportsString);

    const feeLamports = await grpcCalculateFeeLamportsNew({accountKeys, preBalances, postBalances, transactionFeeLamports});

    const dividedFeeLamports = feeLamports / BigInt(trades.length);

    for(const trade of trades){
        trade.feeLamports = dividedFeeLamports;
    }
    return trades;
}

async function grpcCalculateFeeLamportsNew({accountKeys, preBalances, postBalances, transactionFeeLamports}: {accountKeys: Uint8Array[], preBalances: string[], postBalances: string[], transactionFeeLamports: bigint}): Promise<bigint>{
    let feeLamports = transactionFeeLamports;
    for(let i = 0; i < accountKeys.length; i++){
        const accountKey = accountKeys[i];
        if(constants.FEE_PAYING_ADDRESSES.includes(address(bs58.encode(accountKey)))){
            feeLamports += (BigInt(postBalances[i]) - BigInt(preBalances[i]));
        }
    }
    return feeLamports;
}

async function grpcTransactionToPumpFunTrades({signature, accountKeys, instructions, innerInstructionGroups, allBalances, pumpAccountIndex, block}: {signature: string, accountKeys: Uint8Array[], instructions: CompiledInstruction[], innerInstructionGroups: InnerInstructions[], allBalances: TokenBalance[], pumpAccountIndex: number, block: number}): Promise<Trade[]> {
    let trades: Trade[] = [];

    const pumpLogCodecLength = pumpLogCodec.fixedSize;

    const timestamp = Math.floor(Date.now() / 1000);

    for(const innerInstructionGroup of innerInstructionGroups){
        for(const innerInstruction of innerInstructionGroup.instructions){
            try{
                if(innerInstruction.programIdIndex == pumpAccountIndex && innerInstruction.data.length == pumpLogCodecLength){
                    const decoded = pumpLogCodec.decode(innerInstruction.data);
                    trades.push({
                        signature: signature,
                        wallet: address(decoded.user),
                        platform: "pump",
                        direction: decoded.isBuy ? "buy" : "sell",
                        mint: address(decoded.mint),
                        lamports: BigInt(decoded.solAmount),
                        tokens: BigInt(decoded.tokenAmount),
                        feeLamports: 0n,
                        block: block,
                        timestamp: timestamp,
                        amm: undefined,
                        pool1: undefined,
                        pool2: undefined
                    });
                }
            }catch(error){
                logger.error("Error parsing pump.fun anchor log instruction", error);
                continue;
            }
        }
    }

    return trades;

}

async function grpcTransactionToRaydiumTrades({signature, accountKeys, instructions, innerInstructionGroups, allBalances, raydiumAccountIndex, block}: {signature: string, accountKeys: Uint8Array[], instructions: CompiledInstruction[], innerInstructionGroups: InnerInstructions[], allBalances: TokenBalance[], raydiumAccountIndex: number, block: number}): Promise<Trade[]> {
    let trades: Trade[] = [];

    const timestamp = Math.floor(Date.now() / 1000);

    //index of the token program in the account keys (over which transfer instructions are done)
    const tokenProgramIndex = accountKeys.findIndex(account => Buffer.compare(account, constants.TOKEN_PROGRAM_ADDRESS_BYTES) == 0);

    // first traversing over instructions to find trades
    for(let instructionIndex = 0; instructionIndex < instructions.length; instructionIndex++){
        const instruction = instructions[instructionIndex];
        try{
            if(instruction.programIdIndex == raydiumAccountIndex && instruction.data[0] == 9){
                //we now know that instruction is a raydium swap instruction
                const amm = accountKeys[instruction.accounts[1]];
                const pool1 = accountKeys[instruction.accounts[instruction.accounts.length == 18 ? 5 : 4]];
                const pool2 = accountKeys[instruction.accounts[instruction.accounts.length == 18 ? 6 : 5]];
                const wallet = accountKeys[instruction.accounts[instruction.accounts.length == 18 ? 17 : 16]];
                const sourceTokenAccount = accountKeys[instruction.accounts[instruction.accounts.length == 18 ? 15 : 14]];
                const destinationTokenAccount = accountKeys[instruction.accounts[instruction.accounts.length == 18 ? 16 : 15]];

                const ammAddress = address(bs58.encode(amm));
                const pool1Address = address(bs58.encode(pool1));
                const pool2Address = address(bs58.encode(pool2));
                const walletAddress = address(bs58.encode(wallet));

                //getting the inner token account interactions (possibly transfer) instructions for the trade
                let innerTransferInstructions = innerInstructionGroups.find(innerInstructionGroup => innerInstructionGroup.index == instructionIndex)?.instructions.filter(instruction => instruction.programIdIndex == tokenProgramIndex);
                if(innerTransferInstructions == undefined) return [];

                //getting the source and destination token changes
                let sourceTokenChange;
                let destinationTokenChange;
                for(const innerTransferInstruction of innerTransferInstructions){
                    try{
                        const decoded = transferCodec.decode(innerTransferInstruction.data);
                        if(decoded.discriminator == 3){
                            // we now know that this is a transfer instruction
                            if(Buffer.compare(accountKeys[innerTransferInstruction.accounts[0]], sourceTokenAccount) == 0){
                                sourceTokenChange = decoded.amount;
                            }else if(Buffer.compare(accountKeys[innerTransferInstruction.accounts[1]], destinationTokenAccount) == 0){
                                destinationTokenChange = decoded.amount;
                            }
                        }
                    }catch(error){
                        continue;
                    }
                    
                }
                if(sourceTokenChange == undefined || destinationTokenChange == undefined) return [];

                const sourceNotWsol = allBalances.find(balance => balance.owner == String(walletAddress) && balance.mint != String(constants.WSOL_ADDRESS) && balance.accountIndex == instruction.accounts[instruction.accounts.length == 18 ? 15 : 14])?.mint;
                const destinationNotWsol = allBalances.find(balance => balance.owner == String(walletAddress) && balance.mint != String(constants.WSOL_ADDRESS) && balance.accountIndex == instruction.accounts[instruction.accounts.length == 18 ? 16 : 15])?.mint;
                if(sourceNotWsol && destinationNotWsol){
                    // both source and destination are non-wsol tokens, skip
                    continue;
                }
                //check if there exists a non-wsol token in the source or destination
                if(destinationNotWsol){
                    // buy
                    trades.push({
                        signature: signature,
                        wallet: walletAddress,
                        platform: "raydium",
                        direction: "buy",
                        mint: address(destinationNotWsol),
                        lamports: BigInt(sourceTokenChange),
                        tokens: BigInt(destinationTokenChange),
                        feeLamports: 0n,
                        block: block,
                        timestamp: timestamp,
                        amm: ammAddress,
                        pool1: pool1Address,
                        pool2: pool2Address
                    });
                }else if(sourceNotWsol){
                    // sell
                    trades.push({
                        signature: signature,
                        wallet: walletAddress,
                        platform: "raydium",
                        direction: "sell",
                        mint: address(sourceNotWsol),
                        lamports: BigInt(destinationTokenChange),
                        tokens: BigInt(sourceTokenChange),
                        feeLamports: 0n,
                        block: block,
                        timestamp: timestamp,
                        amm: ammAddress,
                        pool1: pool1Address,
                        pool2: pool2Address
                    });
                }else{
                    continue;
                }
                
            }
        }catch(error){
            logger.error("Error parsing raydium swap instruction", error);
            continue;
        }
    }
    //now traversing over inner instructions, possibly hidden swaps
    for(const innerInstructionGroup of innerInstructionGroups){
        for(const innerInstruction of innerInstructionGroup.instructions){
            try{
                if(innerInstruction.programIdIndex == raydiumAccountIndex && innerInstruction.data[0] == 9){
                    // we now know that this is a raydium swap instruction
                    const amm = accountKeys[innerInstruction.accounts[1]];
                    const pool1 = accountKeys[innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 5 : 4]];
                    const pool2 = accountKeys[innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 6 : 5]];
                    const wallet = accountKeys[innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 17 : 16]];
                    const sourceTokenAccount = accountKeys[innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 15 : 14]];
                    const destinationTokenAccount = accountKeys[innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 16 : 15]];

                    const ammAddress = address(bs58.encode(amm));
                    const pool1Address = address(bs58.encode(pool1));
                    const pool2Address = address(bs58.encode(pool2));
                    const walletAddress = address(bs58.encode(wallet));

                    //getting the inner transfer instructions for the trade
                    let innerTransferInstructions = innerInstructionGroup.instructions.filter(instruction => 
                        instruction.programIdIndex == tokenProgramIndex && 
                        instruction.stackHeight && 
                        innerInstruction.stackHeight && 
                        instruction.stackHeight == innerInstruction.stackHeight + 1 
                    );
                    if(innerTransferInstructions == undefined) return [];
                    //getting the source and destination token changes
                    let sourceTokenChange;
                    let destinationTokenChange;
                    for(const innerTransferInstruction of innerTransferInstructions){
                        try{
                            const decoded = transferCodec.decode(innerTransferInstruction.data);
                            if(decoded.discriminator == 3){
                                // we now know that this is a transfer instruction
                                if(Buffer.compare(accountKeys[innerTransferInstruction.accounts[0]], sourceTokenAccount) == 0){
                                    if(sourceTokenChange != undefined) return [];
                                    sourceTokenChange = decoded.amount;
                                }else if(Buffer.compare(accountKeys[innerTransferInstruction.accounts[1]], destinationTokenAccount) == 0){
                                    if(destinationTokenChange != undefined) return [];
                                    destinationTokenChange = decoded.amount;
                                }
                            }
                        }catch(error){
                            continue;
                        }
                        
                    }
                    if(sourceTokenChange == undefined || destinationTokenChange == undefined) return [];

                    const sourceNotWsol = allBalances.find(balance => balance.owner == String(walletAddress) && balance.mint != String(constants.WSOL_ADDRESS) && balance.accountIndex == innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 15 : 14])?.mint;
                    const destinationNotWsol = allBalances.find(balance => balance.owner == String(walletAddress) && balance.mint != String(constants.WSOL_ADDRESS) && balance.accountIndex == innerInstruction.accounts[innerInstruction.accounts.length == 18 ? 16 : 15])?.mint;

                    if(sourceNotWsol && destinationNotWsol){
                        // both source and destination are non-wsol tokens, skip
                        continue;
                    }

                    //check if the source token account is a wsol account
                    if(destinationNotWsol){
                        // buy
                        trades.push({
                            signature: signature,
                            wallet: walletAddress,
                            platform: "raydium",
                            direction: "buy",
                            mint: address(destinationNotWsol),
                            lamports: BigInt(sourceTokenChange),
                            tokens: BigInt(destinationTokenChange),
                            feeLamports: 0n,
                            block: block,
                            timestamp: timestamp,
                            amm: ammAddress,
                            pool1: pool1Address,
                            pool2: pool2Address
                        });
                    }else if(sourceNotWsol){
                        // sell
                        trades.push({
                            signature: signature,
                            wallet: walletAddress,
                            platform: "raydium",
                            direction: "sell",
                            mint: address(sourceNotWsol),
                            lamports: BigInt(destinationTokenChange),
                            tokens: BigInt(sourceTokenChange),
                            feeLamports: 0n,
                            block: block,
                            timestamp: timestamp,
                            amm: ammAddress,
                            pool1: pool1Address,
                            pool2: pool2Address
                        });
                    }
                }
            }catch(error){
                logger.error("Error parsing raydium swap instruction", error);
                continue;
            }
        }
    }
    return trades;
}