/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

import org.bidfp.binary128.tables.FourOverPi;
import org.bidfp.binary128.tables.TrigX;

/**
 * QUAD UX Payne-Hanek radian reduction from {@code dpml_ux_radian_reduce.c}.
 *
 * <p>This is Intel's generated 64-bit digit path: a two-digit fraction is
 * convolved with a four-digit window of 4/pi, extending that window when the
 * reduced value loses significance.
 */
final class UxRadianReduce {
  private static final long BIT_LOSS_ADD = 0x0040_0000_0000_0000L;
  private static final long BIT_LOSS_MASK = 0x3f80_0000_0000_0000L;
  private static final long OCTANT_MASK = 0xe000_0000_0000_0000L;

  private UxRadianReduce() {
  }

  static int reduce(
      Unpacked argument, int octant, Unpacked reduced, StatusFlags status) {
    if (argument.exponent < 0) {
      return reduceSmall(argument, octant, reduced, status);
    }

    long f0 = argument.fracLo;
    long f1 = argument.fracHi;
    int tableOffset = argument.exponent
        - (128 + 2 - FourOverPi.FOUR_OV_PI_ZERO_PAD_LEN);
    int tableIndex = tableOffset >>> 6;
    int bitOffset = tableOffset & 63;

    long g3 = FourOverPi.TABLE[tableIndex];
    long g2 = FourOverPi.TABLE[tableIndex + 1];
    long g1 = FourOverPi.TABLE[tableIndex + 2];
    long g0 = FourOverPi.TABLE[tableIndex + 3];
    long next = FourOverPi.TABLE[tableIndex + 4];
    tableIndex += 5;
    if (bitOffset != 0) {
      int right = 64 - bitOffset;
      g3 = (g3 << bitOffset) | (g2 >>> right);
      g2 = (g2 << bitOffset) | (g1 >>> right);
      g1 = (g1 << bitOffset) | (g0 >>> right);
      g0 = (g0 << bitOffset) | (next >>> right);
    }

    UxScratch.Frame multiplyScratch = UxScratch.acquire();
    try {
      long[] product = multiplyScratch.radianProduct;
      multiplyWindow(f0, f1, g0, g1, g2, g3, product);
      g0 = product[0];
      g1 = product[1];
      g2 = product[2];
      g3 = product[3];
    } finally {
      UxScratch.release(multiplyScratch);
    }
    int signedOctant = argument.sign != 0 ? -octant : octant;
    g3 += (long) signedOctant << 61;

    long extra = 0L;
    int scale = 0;
    while (hasBitLoss(g3)) {
      long digit = next;
      if (tableIndex < FourOverPi.LENGTH) {
        next = FourOverPi.TABLE[tableIndex++];
      } else {
        next = 0L;
      }
      if (bitOffset != 0) {
        digit = (digit << bitOffset) | (next >>> (64 - bitOffset));
      }

      long productLow = digit * f0;
      long productHigh = Wide.umulh(digit, f0);
      extra = productLow;
      long old = productHigh;
      productHigh += g0;
      long carry = Long.compareUnsigned(productHigh, old) < 0 ? 1L : 0L;

      productLow = digit * f1;
      long high = Wide.umulh(digit, f1);
      long low = productLow + productHigh;
      long nextCarry = Long.compareUnsigned(low, productLow) < 0 ? 1L : 0L;
      old = high;
      high += g1;
      long carryOut = Long.compareUnsigned(high, old) < 0 ? 1L : 0L;
      old = high;
      high += carry;
      carryOut |= Long.compareUnsigned(high, old) < 0 ? 1L : 0L;
      old = high;
      high += nextCarry;
      carryOut |= Long.compareUnsigned(high, old) < 0 ? 1L : 0L;
      g0 = low;
      g1 = high;
      if (carryOut != 0L && ++g2 == 0L) {
        g3++;
      }

      long loss = (g2 >>> 55) | (g3 << 9);
      loss ^= loss >> 63;
      if (loss != 0L) {
        break;
      }
      g3 = (g3 & OCTANT_MASK) | (g2 & ~OCTANT_MASK);
      g2 = g1;
      g1 = g0;
      g0 = extra;
      extra = 0L;
      scale += 64;
      if (tableIndex >= FourOverPi.LENGTH) {
        break;
      }
    }

    long quadrant = g3;
    g3 = (g3 << 2) >> 2;
    long signedHigh = g3;
    quadrant -= g3;
    if (g3 == (g3 >> 63)) {
      g3 = g2;
      g2 = g1;
      g1 = g0;
      g0 = extra;
      scale += 64;
    }

    int reducedSign = signedHigh < 0L ? Unpacked.UX_SIGN_BIT : 0;
    if (reducedSign != 0) {
      g3 = ~g3;
      g2 = ~g2;
      g1 = ~g1 + 1L;
      long carry = g1 == 0L ? 1L : 0L;
      long old = g2;
      g2 += carry;
      carry = carry != 0L && Long.compareUnsigned(g2, old) < 0 ? 1L : 0L;
      g3 += carry;
    }

    if (argument.sign != 0) {
      quadrant = -quadrant;
    }
    reduced.setNorm(reducedSign ^ argument.sign, 3, g3, g2);
    UxOps.normalize(reduced);
    int normalization = reduced.exponent - 3;
    if (normalization != 0) {
      reduced.fracLo |= g1 >>> (normalization + 64);
    }
    reduced.exponent -= scale;

    UxScratch.Frame scratch = UxScratch.acquire();
    try {
      Unpacked piOverFour = scratch.unpacked(0);
      piOverFour.copyFrom(UxTable.readUxFloat(TrigX.TABLE, TrigX.UX_PI_OVER_FOUR));
      Unpacked radians = scratch.unpacked(1);
      UxOps.mulUnpacked(reduced, piOverFour, radians, status);
      reduced.copyFrom(radians);
    } finally {
      UxScratch.release(scratch);
    }
    return (int) (quadrant >>> 62);
  }

  private static int reduceSmall(
      Unpacked argument, int octant, Unpacked reduced, StatusFlags status) {
    int effectiveOctant = octant + (argument.sign != 0 ? -1 : 0);
    effectiveOctant += effectiveOctant & 1;
    int quadrant = effectiveOctant >> 1;
    int adjustment = octant - effectiveOctant;
    if (adjustment == 0) {
      reduced.copyFrom(argument);
      return quadrant;
    }

    UxScratch.Frame scratch = UxScratch.acquire();
    try {
      Unpacked piOverFour = scratch.unpacked(0);
      piOverFour.copyFrom(UxTable.readUxFloat(TrigX.TABLE, TrigX.UX_PI_OVER_FOUR));
      if (adjustment < 0) {
        UxOps.negate(piOverFour);
      }
      UxOps.addsubUnpacked(argument, piOverFour, reduced, status);
    } finally {
      UxScratch.release(scratch);
    }
    return quadrant;
  }

  private static boolean hasBitLoss(long mostSignificant) {
    return ((mostSignificant + BIT_LOSS_ADD) & BIT_LOSS_MASK) == 0L;
  }

  private static void multiplyWindow(
      long f0, long f1, long g0, long g1, long g2, long g3, long[] result) {
    Wide.mul128x128(f1, f0, g1, g0, result);
    long w0 = result[3];
    long w1 = result[2];
    long w2 = result[1];
    long w3 = result[0];

    long low = f0 * g2;
    long high = Wide.umulh(f0, g2);
    long old = w2;
    w2 += low;
    long carry = Long.compareUnsigned(w2, old) < 0 ? 1L : 0L;
    w3 += high + carry;
    w3 += f1 * g2;
    w3 += f0 * g3;
    result[0] = w0;
    result[1] = w1;
    result[2] = w2;
    result[3] = w3;
  }
}
