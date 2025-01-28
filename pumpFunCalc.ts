

export function getBuyOutAmount(amount: bigint, virtualSolReserves: bigint, virtualTokenReserves: bigint): bigint {
    if (amount <= 0n) {
      return 0n;
    }

    // Calculate the product of virtual reserves
    let n = virtualSolReserves * virtualTokenReserves;

    // Calculate the new virtual sol reserves after the purchase
    let i = virtualSolReserves + amount;

    // Calculate the new virtual token reserves after the purchase
    let r = n / i + 1n;

    // Calculate the amount of tokens to be purchased
    let s = virtualTokenReserves - r;

    // Return the minimum of the calculated tokens and real token reserves
    //return s < this.realTokenReserves ? s : this.realTokenReserves;
    return s;
  }

export function getSellOutAmount(amount: bigint, virtualSolReserves: bigint, virtualTokenReserves: bigint, feeBasisPoints: bigint = 100n): bigint {
    if (amount <= 0n) {
      return 0n;
    }

    // Calculate the proportional amount of virtual sol reserves to be received
    let n =
      (amount * virtualSolReserves) / (virtualTokenReserves + amount);

    // Calculate the fee amount in the same units
    let a = (n * feeBasisPoints) / 10000n;

    // Return the net amount after deducting the fee
    return n - a;
  }

console.log(getSellOutAmount(1000000000000000n, 30016813335n, 1072398982151541n));