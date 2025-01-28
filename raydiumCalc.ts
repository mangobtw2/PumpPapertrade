/**
 * Calculate how many coin tokens you get when swapping tokens into the pool.
 *
 * @param inPool    Current in coin balance in the pool
 * @param outPool   Current out coin balance in the pool
 * @param amountIn  The in coin tokens you are sending *before* fees
 * @param feeRate   The swap fee rate, e.g. 0.003 (0.3%)
 * @returns Number of out coin tokens you would get out
 */
export function getOutAmount(
  inPool: bigint,
  outPool: bigint,
  amountIn: bigint,
  feeRate: number = 0.0025
): bigint {
  // 1) Subtract the fee
  const feeRateTimes10000 = BigInt(feeRate * 10000);
  const amountInAfterFee = (amountIn * (10000n - feeRateTimes10000)) / 10000n;

  // 2) Apply the constant-product swap formula:
  //    coinOut = (coinPool * amountInAfterFee) / (pcPool + amountInAfterFee)
  return (outPool * amountInAfterFee) / (inPool + amountInAfterFee);
}

