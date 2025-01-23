import { FullySignedTransaction, getBase64EncodedWireTransaction} from "@solana/web3.js";
import * as constants from "./constants.js";
import * as config from "../config.js";
import { createLogger } from "../logger.js";

const logger = createLogger(import.meta.url);

// Track the timestamps of recent requests
const requestTimestamps: number[] = [];
const MAX_REQUESTS_PER_SECOND = config.nozomi_urls.length * 5;
let blockEngineIndex = 0;

// Helper function to enforce rate limit
async function enforceRateLimit() {
    const now = Date.now();
    // Remove timestamps older than 1 second
    while (requestTimestamps.length > 0 && requestTimestamps[0] < now - 1000) {
        requestTimestamps.shift();
    }

    if (requestTimestamps.length >= 8){
        logger.warn("Nozomi block engine rate limit warning: more than 8 transactions in the queue")
    }
    
    if (requestTimestamps.length >= MAX_REQUESTS_PER_SECOND) {
        // Wait until we can make another request
        const oldestRequest = requestTimestamps[0];
        const waitTime = oldestRequest + 1000 - now;
        await new Promise(resolve => setTimeout(resolve, waitTime));
    }
    
    requestTimestamps.push(Date.now());
}

//sends a transaction to nozomi. does not return anything even on failure (we will rely on rpc in case of failure). 3 retries.
export async function sendTransaction(transaction: FullySignedTransaction){
    try{
        await enforceRateLimit();
    
        const base64EncodedTransaction = getBase64EncodedWireTransaction(transaction);
        for(let attempt = 0; attempt < 3; attempt++){
            try{
                const blockEngineUrl = config.nozomi_urls[blockEngineIndex];
                blockEngineIndex = (blockEngineIndex + 1) % config.nozomi_urls.length;
                const response = await fetch(blockEngineUrl, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify({
                        id: 1,
                        jsonrpc: "2.0",
                        method: "sendTransaction",
                        params: [
                            base64EncodedTransaction,
                            {
                                "encoding": "base64"
                            }
                        ]
                    })
                })

                const data = await response.json();
                if(data.result){
                    logger.debug("Transaction sent to nozomi:", data.result);
                    return;
                }
            }catch(error){
                logger.error("Error sending transaction to nozomi", error);
            }
        }
    } catch(error){
        logger.error("Error base64 encoding transaction", error);
    }
}
