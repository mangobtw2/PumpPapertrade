import { createSolanaRpc, createSolanaRpcFromTransport, createDefaultRpcTransport, type RpcTransport, PendingRpcRequest, createSolanaRpcSubscriptions} from '@solana/web3.js';
import { rpcs, performance_rpcs, ws_url } from '../config.js';
import { createLogger } from '../logger.js';
const MAX_ATTEMPTS_BASIC = 3;
const MAX_ATTEMPTS_PERFORMANCE = 6;

const logger = createLogger(import.meta.url);

interface RPCNode {
    transport: RpcTransport;
    consecutiveFailures: number;
    nextAvailableTime: number; // ms timestamp
  }

const performanceNodes: RPCNode[] = performance_rpcs.map(url => ({
    transport: createDefaultRpcTransport({url}),
    consecutiveFailures: 0,
    nextAvailableTime: 0
}))

const nodes: RPCNode[] = rpcs.map(url => ({
    transport: createDefaultRpcTransport({url}),
    consecutiveFailures: 0,
    nextAvailableTime: 0
}))

let basicIndex = 0;
let performanceIndex = 0;


// Transport selection functions

/**
 * Selects the next available performance transport index in a round-robin fasion.
 * @returns The selected transport, or the index of the transport if all are unavailable.
 */
function selectPerformanceTransportIndex():number {
    for(let i = 0; i < performanceNodes.length; i++) {
        performanceIndex = (performanceIndex + 1) % performanceNodes.length;
        const node = performanceNodes[performanceIndex];
        if(node.nextAvailableTime < Date.now()) {
            return performanceIndex;
        }
    }
    return performanceIndex;
}

/**
 * Selects the next available basic transport indexin a round-robin fasion.
 * @returns The selected transport, or -1 if all are unavailable. !!IMPORTANT!!
 */
function selectBasicTransportIndex():number {
    for (let i = 0; i < nodes.length; i++) {
        basicIndex = (basicIndex + 1) % nodes.length;
        const node = nodes[basicIndex];
        if (node.nextAvailableTime < Date.now()) {
            return basicIndex;
        }
    }
    return -1;
}



// Transport functions

async function performanceTransport<TResponse>(...args: Parameters<RpcTransport>): Promise<TResponse> {
    const payload = args[0].payload;
    logger.trace("Payload in performance transport", payload)

    let requestError;
    for(let i = 0; i < MAX_ATTEMPTS_PERFORMANCE; i++) {
        const transportIndex = selectPerformanceTransportIndex();
        const transport = performanceNodes[transportIndex].transport;
        try {
            //const res = await withTimeout(transport(...args) as Promise<TResponse>, TIMEOUT);
            const res = await transport(...args) as TResponse;
            logger.trace("success in performance transport using transport "+transportIndex, res)
            registerSuccess(transportIndex, "performance");
            return res;
        } catch(error) {
            logger.trace("error in performance transport using transport "+transportIndex, error)
            requestError = error;
            registerFailure(transportIndex, "performance");
        }
    }
    throw requestError;
}

async function basicTransport<TResponse>(...args: Parameters<RpcTransport>): Promise<TResponse> {
    const payload = args[0].payload;

    let requestError;
    for(let i = 0; i < MAX_ATTEMPTS_BASIC; i++) {
        const transportIndex = selectBasicTransportIndex();
        // If all basic nodes are in timeout, use performance nodes
        if(transportIndex === -1) {
            break;
        }
        const transport = nodes[transportIndex].transport;
        try {
            const res = await transport(...args) as TResponse;
            registerSuccess(transportIndex, "basic");
            return res;
        } catch(error) {
            requestError = error;
            registerFailure(transportIndex, "basic");
        }
    }
    logger.trace("All basic nodes failed, using performance nodes")
    try {
        return await performanceTransport(...args) as TResponse;
    } catch(performanceError) {
        throw new Error(`Basic nodes failed with: ${requestError}. Performance nodes failed with: ${performanceError}`);
    }
}

export const basicRpc = createSolanaRpcFromTransport(basicTransport);
export const performanceRpc = createSolanaRpcFromTransport(performanceTransport);
export const websocketRpc = createSolanaRpcSubscriptions(ws_url);


// Utils

function calculateBackoffDuration(consecutiveFailures: number): number {
    const base = 500 * Math.pow(2, consecutiveFailures);
    // Add random jitter between -12.5% to +12.5% of the base value
    const jitter = base * (0.5 - Math.random()) / 4;
    return Math.max(0, Math.floor(base + jitter));
}

function registerFailure(nodeIndex: number, type: "basic" | "performance") {
    const nodeList = type === "basic" ? nodes : performanceNodes;
    const node = nodeList[nodeIndex];
    node.consecutiveFailures++;
    if(node.consecutiveFailures === 5) {
        logger.warn("Node "+nodeIndex+" failed 5 times in a row");
    }
    node.nextAvailableTime = Date.now() + calculateBackoffDuration(node.consecutiveFailures);
}

function registerSuccess(nodeIndex: number, type: "basic" | "performance") {
    const nodeList = type === "basic" ? nodes : performanceNodes;
    const node = nodeList[nodeIndex];
    node.consecutiveFailures = 0;
    node.nextAvailableTime = 0;
}

// function withTimeout<T>(transportPromise: Promise<T>, timeout: number): Promise<T> {
//     let timeoutId: NodeJS.Timeout;
//     const timeoutPromise = new Promise<T>((_, reject) => {
//         timeoutId = setTimeout(() => {
//             reject(new Error("Request timed out"));
//         }, timeout);
//     });

//     return Promise.race([
//         transportPromise.finally(() => clearTimeout(timeoutId)),
//         timeoutPromise
//     ]);
// }
