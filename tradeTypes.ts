import { Address } from "@solana/web3.js";

export type Trade = {
    signature: string;
    wallet: Address;
    platform: "raydium" | "pump";
    direction: "buy" | "sell";
    mint: Address;
    lamports: bigint;
    tokens: bigint;
    feeLamports: bigint;
    block: number;
    timestamp: number;
    amm: Address | undefined;
    pool1: Address | undefined;
    pool2: Address | undefined;
}