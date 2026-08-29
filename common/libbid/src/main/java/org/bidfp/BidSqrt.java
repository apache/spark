/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

final class BidSqrt {
  private static final long MAX_COEFFICIENT_128_HIGH = 0x0001_ed09_bead_87c0L;
  private static final long MAX_COEFFICIENT_128_LOW = 0x378d_8e63_ffff_ffffL;
  private static final long TEN_34_HIGH = 0x0001_ed09_bead_87c0L;
  private static final long TEN_34_LOW = 0x378d_8e64_0000_0000L;

  private BidSqrt() {
  }

  static long sqrt64(long x, RoundingMode mode, StatusFlags flags) {
    if (Bid64Raw.isNaN(x)) {
      return BidIntegral.canonicalizeNaN64(x, flags);
    }
    if (Bid64Raw.isInf(x)) {
      if (Bid64Raw.isSigned(x)) {
        flags.raise(StatusFlags.INVALID);
        return Bid64.MASK_NAN;
      }
      return Bid64.MASK_INFINITY;
    }
    if (Bid64Raw.isZero(x)) {
      int exp = Bid64.biasedExponentBits(x) - 398;
      int resultExp = exp >> 1;
      return Bid64.finiteRawBits(Bid64Raw.isSigned(x), resultExp + 398, 0L);
    }
    if (Bid64Raw.isSigned(x)) {
      flags.raise(StatusFlags.INVALID);
      return Bid64.MASK_NAN;
    }
    long coefficient = Bid64.significandBits(x);
    int exp = Bid64.biasedExponentBits(x) - 398;
    if ((exp & 1) != 0) {
      coefficient *= 10;
      exp--;
    }
    return sqrt64Finite(coefficient, exp, mode, flags);
  }

  private static long sqrt64Finite(
      long coefficient, int exp, RoundingMode mode, StatusFlags flags) {
    long pairs = 0;
    int pairCount = 0;
    long work = coefficient;
    while (work != 0) {
      long quotient = work / 100;
      pairs |= (work - quotient * 100) << (pairCount * 7);
      work = quotient;
      pairCount++;
    }
    if (pairCount > 9) {
      return sqrt64DecNum(coefficient, exp, mode, flags);
    }

    long root = 0;
    long remainder = 0;
    for (int position = 0; position < 16; position++) {
      int pairIndex = pairCount - position - 1;
      long pair = pairIndex >= 0 ? (pairs >>> (pairIndex * 7)) & 0x7fL : 0;
      remainder = remainder * 100 + pair;
      int digit = 9;
      for (; digit > 0; digit--) {
        long candidate = (20 * root + digit) * digit;
        if (candidate <= remainder) {
          remainder -= candidate;
          break;
        }
      }
      root = root * 10 + digit;
    }

    boolean exact = remainder == 0;
    if (!exact) {
      if (incrementSqrt64(root, remainder, mode)) {
        root++;
      }
      flags.raise(StatusFlags.INEXACT);
    }
    int resultExp = exp / 2 - (16 - pairCount);
    if (root == 10_000_000_000_000_000L) {
      root /= 10;
      resultExp++;
    }
    if (exact) {
      int maximumExp = exp / 2;
      while (root % 10 == 0 && resultExp < maximumExp) {
        root /= 10;
        resultExp++;
      }
    }
    return Bid64.finiteRawBits(false, resultExp + 398, root);
  }

  static void sqrt128(
      long high, long low, RoundingMode mode, StatusFlags flags, long[] out) {
    if ((high & Bid128.MASK_NAN) == Bid128.MASK_NAN) {
      BidIntegral.canonicalizeNaN128(high, low, flags, out);
      return;
    }
    boolean negative = (high & Bid128.MASK_SIGN) != 0;
    if ((high & Bid128.MASK_INFINITY) == Bid128.MASK_INFINITY) {
      if (negative) {
        flags.raise(StatusFlags.INVALID);
        out[0] = Bid128.MASK_NAN;
        out[1] = 0;
      } else {
        out[0] = Bid128.MASK_INFINITY;
        out[1] = 0;
      }
      return;
    }

    int biasedExp = (high & Bid128.MASK_STEERING_BITS) == Bid128.MASK_STEERING_BITS
        ? (int) ((high >>> 47) & 0x3fffL)
        : (int) ((high & Bid128.MASK_EXPONENT) >>> 49);
    long coefficientHigh = high & Bid128.MASK_COEFFICIENT;
    boolean canonical = (high & Bid128.MASK_STEERING_BITS) != Bid128.MASK_STEERING_BITS
        && compare128(
            coefficientHigh,
            low,
            MAX_COEFFICIENT_128_HIGH,
            MAX_COEFFICIENT_128_LOW) <= 0;
    if (!canonical || (coefficientHigh | low) == 0) {
      int resultExp = (biasedExp - 6176) >> 1;
      out[0] = (negative ? Bid128.MASK_SIGN : 0L)
          | ((long) (resultExp + 6176) << 49);
      out[1] = 0;
      return;
    }
    if (negative) {
      flags.raise(StatusFlags.INVALID);
      out[0] = Bid128.MASK_NAN;
      out[1] = 0;
      return;
    }

    int exp = biasedExp - 6176;
    if ((exp & 1) != 0) {
      coefficientHigh = coefficientHigh * 10 + unsignedMultiplyHigh(low, 10);
      low *= 10;
      exp--;
    }
    sqrt128Finite(coefficientHigh, low, exp, mode, flags, out);
  }

  private static void sqrt128Finite(
      long coefficientHigh,
      long coefficientLow,
      int exp,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    long pairsHigh = 0;
    long pairsLow = 0;
    int pairCount = 0;
    long workHigh = coefficientHigh;
    long workLow = coefficientLow;
    while ((workHigh | workLow) != 0) {
      long quotientHigh = 0;
      long quotientLow = 0;
      long remainder = 0;
      for (int limb = 3; limb >= 0; limb--) {
        long digit = limb >= 2
            ? (workHigh >>> ((limb - 2) * 32)) & 0xffff_ffffL
            : (workLow >>> (limb * 32)) & 0xffff_ffffL;
        long dividend = (remainder << 32) | digit;
        long quotientDigit = dividend / 100;
        remainder = dividend - quotientDigit * 100;
        if (limb >= 2) {
          quotientHigh |= quotientDigit << ((limb - 2) * 32);
        } else {
          quotientLow |= quotientDigit << (limb * 32);
        }
      }
      int shift = pairCount * 7;
      if (shift < 64) {
        pairsLow |= remainder << shift;
        if (shift > 57) {
          pairsHigh |= remainder >>> (64 - shift);
        }
      } else {
        pairsHigh |= remainder << (shift - 64);
      }
      workHigh = quotientHigh;
      workLow = quotientLow;
      pairCount++;
    }
    if (pairCount > 18) {
      sqrt128DecNum(coefficientHigh, coefficientLow, exp, mode, flags, out);
      return;
    }

    long rootHigh = 0;
    long rootLow = 0;
    long remainderHigh = 0;
    long remainderLow = 0;
    for (int position = 0; position < 34; position++) {
      int pairIndex = pairCount - position - 1;
      long pair = pairIndex >= 0 ? pair128(pairsHigh, pairsLow, pairIndex) : 0;
      remainderHigh =
          remainderHigh * 100 + unsignedMultiplyHigh(remainderLow, 100);
      remainderLow = remainderLow * 100 + pair;
      if (Long.compareUnsigned(remainderLow, pair) < 0) {
        remainderHigh++;
      }

      int digit = 9;
      for (; digit > 0; digit--) {
        long baseLow = rootLow * 20 + digit;
        long baseHigh = rootHigh * 20 + unsignedMultiplyHigh(rootLow, 20);
        if (Long.compareUnsigned(baseLow, digit) < 0) {
          baseHigh++;
        }
        long candidateLow = baseLow * digit;
        long candidateHigh =
            baseHigh * digit + unsignedMultiplyHigh(baseLow, digit);
        if (compare128(
            candidateHigh, candidateLow, remainderHigh, remainderLow) <= 0) {
          long oldRemainderLow = remainderLow;
          remainderLow -= candidateLow;
          remainderHigh -= candidateHigh;
          if (Long.compareUnsigned(oldRemainderLow, candidateLow) < 0) {
            remainderHigh--;
          }
          break;
        }
      }
      long oldRootLow = rootLow;
      rootLow = rootLow * 10 + digit;
      rootHigh = rootHigh * 10 + unsignedMultiplyHigh(oldRootLow, 10);
      if (Long.compareUnsigned(rootLow, digit) < 0) {
        rootHigh++;
      }
    }

    boolean exact = (remainderHigh | remainderLow) == 0;
    if (!exact) {
      if (incrementSqrt128(
          rootHigh, rootLow, remainderHigh, remainderLow, mode)) {
        rootLow++;
        if (rootLow == 0) {
          rootHigh++;
        }
      }
      flags.raise(StatusFlags.INEXACT);
    }
    int resultExp = exp / 2 - (34 - pairCount);
    if (rootHigh == TEN_34_HIGH && rootLow == TEN_34_LOW) {
      rootHigh = 0x0000_314d_c644_8d93L;
      rootLow = 0x38c1_5b0a_0000_0000L;
      resultExp++;
    }
    if (exact) {
      int maximumExp = exp / 2;
      while (resultExp < maximumExp) {
        long quotientHigh = 0;
        long quotientLow = 0;
        long remainder = 0;
        for (int limb = 3; limb >= 0; limb--) {
          long source = limb >= 2
              ? (rootHigh >>> ((limb - 2) * 32)) & 0xffff_ffffL
              : (rootLow >>> (limb * 32)) & 0xffff_ffffL;
          long dividend = (remainder << 32) | source;
          long quotientDigit = dividend / 10;
          remainder = dividend - quotientDigit * 10;
          if (limb >= 2) {
            quotientHigh |= quotientDigit << ((limb - 2) * 32);
          } else {
            quotientLow |= quotientDigit << (limb * 32);
          }
        }
        if (remainder != 0) {
          break;
        }
        rootHigh = quotientHigh;
        rootLow = quotientLow;
        resultExp++;
      }
    }
    out[0] = ((long) (resultExp + 6176) << 49) | rootHigh;
    out[1] = rootLow;
  }

  private static long pair128(long high, long low, int index) {
    int shift = index * 7;
    if (shift < 58) {
      return (low >>> shift) & 0x7fL;
    }
    if (shift < 64) {
      return ((low >>> shift) | (high << (64 - shift))) & 0x7fL;
    }
    return (high >>> (shift - 64)) & 0x7fL;
  }

  private static boolean incrementSqrt64(
      long root, long remainder, RoundingMode mode) {
    if (mode == RoundingMode.TOWARD_POSITIVE) {
      return true;
    }
    if (mode == RoundingMode.TOWARD_NEGATIVE || mode == RoundingMode.TOWARD_ZERO) {
      return false;
    }
    return 4 * remainder > 4 * root + 1;
  }

  private static boolean incrementSqrt128(
      long rootHigh,
      long rootLow,
      long remainderHigh,
      long remainderLow,
      RoundingMode mode) {
    if (mode == RoundingMode.TOWARD_POSITIVE) {
      return true;
    }
    if (mode == RoundingMode.TOWARD_NEGATIVE || mode == RoundingMode.TOWARD_ZERO) {
      return false;
    }
    long leftHigh = (remainderHigh << 2) | (remainderLow >>> 62);
    long leftLow = remainderLow << 2;
    long rightHigh = (rootHigh << 2) | (rootLow >>> 62);
    long rightLow = (rootLow << 2) + 1;
    if (rightLow == 0) {
      rightHigh++;
    }
    return compare128(leftHigh, leftLow, rightHigh, rightLow) > 0;
  }

  private static int compare128(
      long leftHigh, long leftLow, long rightHigh, long rightLow) {
    int highComparison = Long.compareUnsigned(leftHigh, rightHigh);
    return highComparison != 0
        ? highComparison
        : Long.compareUnsigned(leftLow, rightLow);
  }

  private static long unsignedMultiplyHigh(long left, long right) {
    long high = Math.multiplyHigh(left, right);
    if (left < 0) {
      high += right;
    }
    if (right < 0) {
      high += left;
    }
    return high;
  }

  private static long sqrt64DecNum(
      long coefficient, int exp, RoundingMode mode, StatusFlags flags) {
    DecNum radicand = DecNum.ofLong(coefficient);
    int scale = 16 - (radicand.digitCount() + 1) / 2;
    radicand.multiplyPow10(2 * scale);
    DecNum.Sqrt sqrt = DecNum.sqrtFloor(radicand);
    DecNum result = sqrt.root();
    boolean exact = sqrt.remainder().isZero();
    if (!exact) {
      if (incrementSqrt(result, sqrt.remainder(), mode)) {
        result.addOne();
      }
      flags.raise(StatusFlags.INEXACT);
    }
    result.shiftExp(exp / 2 - scale);
    if (exact) {
      result.stripTrailingZeros(exp / 2);
    }
    return result.packBid64(RoundingMode.TOWARD_ZERO, new StatusFlags());
  }

  private static void sqrt128DecNum(
      long coefficientHigh,
      long coefficientLow,
      int exp,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    DecNum radicand = DecNum.ofUnsigned(coefficientHigh, coefficientLow);
    int scale = 34 - (radicand.digitCount() + 1) / 2;
    radicand.multiplyPow10(2 * scale);
    DecNum.Sqrt sqrt = DecNum.sqrtFloor(radicand);
    DecNum result = sqrt.root();
    boolean exact = sqrt.remainder().isZero();
    if (!exact) {
      if (incrementSqrt(result, sqrt.remainder(), mode)) {
        result.addOne();
      }
      flags.raise(StatusFlags.INEXACT);
    }
    result.shiftExp(exp / 2 - scale);
    if (exact) {
      result.stripTrailingZeros(exp / 2);
    }
    result.packBid128(RoundingMode.TOWARD_ZERO, new StatusFlags(), out);
  }

  private static boolean incrementSqrt(
      DecNum root, DecNum remainder, RoundingMode mode) {
    if (mode == RoundingMode.TOWARD_POSITIVE) {
      return true;
    }
    if (mode == RoundingMode.TOWARD_NEGATIVE || mode == RoundingMode.TOWARD_ZERO) {
      return false;
    }
    DecNum left = new DecNum();
    left.copyFrom(remainder);
    left.multiplySmall(4);
    DecNum right = new DecNum();
    right.copyFrom(root);
    right.multiplySmall(4);
    right.addOne();
    return left.compareAbsolute(right) > 0;
  }
}
