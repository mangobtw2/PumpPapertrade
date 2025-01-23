import { address } from "@solana/addresses";
import { Address } from "@solana/web3.js";
import bs58 from 'bs58';
export const RANDOM_ADDRESS = address("2ZyqjqRMc7swFdXa2tC3tbzkJQVs6pEmUQ1f1zRQ5AA3") //filler address for not needed addresses

export const WSOL_ADDRESS = address("So11111111111111111111111111111111111111112")

export const TOKEN_PROGRAM_ADDRESS = address("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
export const ASSOCIATED_TOKEN_PROGRAM_ADDRESS = address("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
export const SYSTEM_PROGRAM_ADDRESS = address("11111111111111111111111111111111")
export const RENT_PROGRAM_ADDRESS = address("SysvarRent111111111111111111111111111111111")

export const SYSTEM_PROGRAM_ADDRESS_BYTES = bs58.decode(String(SYSTEM_PROGRAM_ADDRESS))
export const TOKEN_PROGRAM_ADDRESS_BYTES = bs58.decode(String(TOKEN_PROGRAM_ADDRESS))

// RAYDIUM
export const RAYDIUM_PROGRAM_ADDRESS = address("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");
export const RAYDIUM_AUTHORITY_ADDRESS = address("5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1")

export const RAYDIUM_PROGRAM_ADDRESS_BYTES = bs58.decode(String(RAYDIUM_PROGRAM_ADDRESS))

// OPENBOOK
export const OPENBOOK_PROGRAM_ADDRESS = address("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX")

// PUMP.FUN
export const PUMP_FUN_PROGRAM_ADDRESS = address("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
export const PUMP_FUN_GLOBAL_ADDRESS = address("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf")
export const PUMP_FUN_FEE_RECEPIENT = address("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM")
export const PUMP_FUN_EVENT_AUTHORITY = address("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1")
export const PUMP_FUN_MIGRATION_ADDRESS = address("39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg")

export const PUMP_FUN_PROGRAM_ADDRESS_BYTES = bs58.decode(String(PUMP_FUN_PROGRAM_ADDRESS))
export const PUMP_FUN_FEE_RECEPIENT_BYTES = bs58.decode(String(PUMP_FUN_FEE_RECEPIENT))

// JITO
export const JITO_TIP_ADDRESSES: Address[] = [
    address("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5"),
    address("HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe"),
    address("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY"),
    address("ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49"),
    address("DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh"),
    address("ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt"),
    address("DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL"),
    address("3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT")
]

// NOZOMI
export const NOZOMI_TIP_ADDRESSES: Address[] = [
    address("TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq"),
    address("noz3jAjPiHuBPqiSPkkugaJDkJscPuRhYnSpbi8UvC4"),
    address("noz3str9KXfpKknefHji8L1mPgimezaiUyCHYMDv1GE"),
    address("noz6uoYCDijhu1V7cutCpwxNiSovEwLdRHPwmgCGDNo"),
    address("noz9EPNcT7WH6Sou3sr3GGjHQYVkN3DNirpbvDkv9YJ"),
    address("nozc5yT15LazbLTFVZzoNZCwjh3yUtW86LoUyqsBu4L"),
    address("nozFrhfnNGoyqwVuwPAW4aaGqempx4PU6g6D9CJMv7Z"),
    address("nozievPk7HyK1Rqy1MPJwVQ7qQg2QoJGyP71oeDwbsu"),
    address("noznbgwYnBLDHu8wcQVCEw6kDrXkPdKkydGJGNXGvL7"),
    address("nozNVWs5N8mgzuD3qigrCG2UoKxZttxzZ85pvAQVrbP"),
    address("nozpEGbwx4BcGp6pvEdAh1JoC2CQGZdU6HbNP1v2p6P"),
    address("nozrhjhkCr3zXT3BiT4WCodYCUFeQvcdUkM7MqhKqge"),
    address("nozrwQtWhEdrA6W8dkbt9gnUaMs52PdAv5byipnadq3"),
    address("nozUacTVWub3cL4mJmGCYjKZTnE9RbdY5AP46iQgbPJ"),
    address("nozWCyTPppJjRuw2fpzDhhWbW355fzosWSzrrMYB1Qk"),
    address("nozWNju6dY353eMkMqURqwQEoM3SFgEKC6psLCSfUne"),
    address("nozxNBgWohjR75vdspfxR5H9ceC7XXH99xpxhVGt3Bb")
]

// TRANSACTION PARSING
export const FEE_PAYING_ADDRESSES: Address[] = JITO_TIP_ADDRESSES.concat([PUMP_FUN_FEE_RECEPIENT]).concat(NOZOMI_TIP_ADDRESSES)