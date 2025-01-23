import { Trade } from "./analysis/basic.js";
import { Address } from "@solana/web3.js";
import { getBondingCurveAddress } from "./coreUtils.js";
import {rpc} from "./core/index.js"

async function isFromPump(mint: string){
    const bondingCurveAddress = await getBondingCurveAddress({mintPubkey: mint as Address});
    const existTradesOnChain = await rpc.performanceRpc.getSignaturesForAddress(bondingCurveAddress, {
        limit: 1
    }).send();
    return existTradesOnChain.length > 0;
}

export async function checkRaydium(trades: Trade[]){
    const tradedMints = trades.map(trade => trade.mint);
    //check 1: if no pump trades, filter out
    let existsPump = tradedMints.some(mint => mint.endsWith('pump'));
    if(!existsPump){
        return false;
    }
    // check 2: check 3 pump mints if they are actually from pump.fun
    const pumpMints = tradedMints.filter(mint => mint.endsWith('pump'));
    const pumpMintsSet = new Set(pumpMints);
    let iteration = 0;
    for(const mint of pumpMintsSet){
        const isPump = await isFromPump(mint);
        if(!isPump){
            return false;
        }
        iteration++;
        if(iteration >= 3){
            return true;
        }
    }
    return true;
}