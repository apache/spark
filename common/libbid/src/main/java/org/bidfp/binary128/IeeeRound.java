/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

import java.math.BigInteger;

/** Exact integer rounding shared by packed binary128 arithmetic and conversion. */
final class IeeeRound {
  private static final BigInteger TWO_112 = BigInteger.ONE.shiftLeft(112);
  private static final BigInteger TWO_113 = BigInteger.ONE.shiftLeft(113);
  private static final BigInteger TWO_52 = BigInteger.ONE.shiftLeft(52);
  private static final BigInteger TWO_53 = BigInteger.ONE.shiftLeft(53);

  private IeeeRound() {
  }

  static final class Finite {
    final boolean negative;
    final BigInteger significand;
    final int exponent;

    Finite(boolean negative, BigInteger significand, int exponent) {
      this.negative = negative;
      this.significand = significand;
      this.exponent = exponent;
    }
  }

  static Finite decode(Binary128 x) {
    int biased = x.biasedExponent();
    BigInteger fraction = Wide.u128(x.fractionHigh(), x.fractionLow());
    if (biased == 0) {
      return new Finite(x.isSigned(), fraction, -16494);
    }
    return new Finite(
        x.isSigned(), fraction.or(TWO_112), biased - Binary128.BIAS - 112);
  }

  static Binary128 binary128(
      boolean negative,
      BigInteger numerator,
      BigInteger denominator,
      int exponent,
      RoundingMode mode,
      StatusFlags status) {
    if (numerator.signum() == 0) {
      return negative ? Binary128.NEGATIVE_ZERO : Binary128.ZERO;
    }
    int topExponent = floorLog2(numerator, denominator, exponent);
    if (topExponent > 16383) {
      return overflow128(negative, mode, status);
    }

    Rounded rounded;
    int biased;
    if (topExponent >= -16382) {
      rounded = quotient(numerator, denominator, exponent - (topExponent - 112),
          negative, mode);
      if (rounded.value.equals(TWO_113)) {
        rounded = new Rounded(TWO_112, rounded.inexact);
        topExponent++;
      }
      if (topExponent > 16383) {
        return overflow128(negative, mode, status);
      }
      biased = topExponent + Binary128.BIAS;
    } else {
      rounded = quotient(numerator, denominator, exponent + 16494, negative, mode);
      if (rounded.value.compareTo(TWO_112) >= 0) {
        rounded = new Rounded(TWO_112, rounded.inexact);
        biased = 1;
      } else {
        biased = 0;
      }
    }

    if (rounded.inexact) {
      status.raise(StatusFlags.INEXACT);
      if (biased == 0) {
        status.raise(StatusFlags.UNDERFLOW);
      }
    }
    if (rounded.value.signum() == 0) {
      return negative ? Binary128.NEGATIVE_ZERO : Binary128.ZERO;
    }
    BigInteger fraction = biased == 0 ? rounded.value : rounded.value.clearBit(112);
    long[] limbs = new long[2];
    Wide.toU128(fraction, limbs);
    return Binary128.fromFields(negative, biased, limbs[0], limbs[1]);
  }

  static Binary128 sqrt(Binary128 x, RoundingMode mode, StatusFlags status) {
    Finite finite = decode(x);
    BigInteger significand = finite.significand;
    int exponent = finite.exponent;
    int valueTop = significand.bitLength() - 1 + exponent;
    int rootTop = Math.floorDiv(valueTop, 2);
    int targetExponent = rootTop - 112;
    int shift = exponent - 2 * targetExponent;
    if (shift < 0) {
      throw new ArithmeticException("internal square-root scaling");
    }
    BigInteger radicand = significand.shiftLeft(shift);
    BigInteger root = radicand.sqrt();
    BigInteger remainder = radicand.subtract(root.multiply(root));
    boolean inexact = remainder.signum() != 0;
    if (inexact && incrementSqrt(root, radicand, finite.negative, mode)) {
      root = root.add(BigInteger.ONE);
    }
    if (root.equals(TWO_113)) {
      root = TWO_112;
      rootTop++;
    }
    if (inexact) {
      status.raise(StatusFlags.INEXACT);
    }
    BigInteger fraction = root.clearBit(112);
    long[] limbs = new long[2];
    Wide.toU128(fraction, limbs);
    return Binary128.fromFields(false, rootTop + Binary128.BIAS, limbs[0], limbs[1]);
  }

  static double binary64(Binary128 x, RoundingMode mode, StatusFlags status) {
    Finite finite = decode(x);
    if (finite.significand.signum() == 0) {
      return finite.negative ? -0.0d : 0.0d;
    }
    int topExponent = floorLog2(finite.significand, BigInteger.ONE, finite.exponent);
    if (topExponent > 1023) {
      return overflow64(finite.negative, mode, status);
    }

    Rounded rounded;
    int biased;
    if (topExponent >= -1022) {
      rounded = quotient(finite.significand, BigInteger.ONE,
          finite.exponent - (topExponent - 52), finite.negative, mode);
      if (rounded.value.equals(TWO_53)) {
        rounded = new Rounded(TWO_52, rounded.inexact);
        topExponent++;
      }
      if (topExponent > 1023) {
        return overflow64(finite.negative, mode, status);
      }
      biased = topExponent + 1023;
    } else {
      rounded = quotient(finite.significand, BigInteger.ONE,
          finite.exponent + 1074, finite.negative, mode);
      if (rounded.value.compareTo(TWO_52) >= 0) {
        rounded = new Rounded(TWO_52, rounded.inexact);
        biased = 1;
      } else {
        biased = 0;
      }
    }

    if (rounded.inexact) {
      status.raise(StatusFlags.INEXACT);
      if (biased == 0) {
        status.raise(StatusFlags.UNDERFLOW);
      }
    }
    long fraction = rounded.value.longValue();
    if (biased != 0) {
      fraction &= 0x000f_ffff_ffff_ffffL;
    }
    long bits = ((long) biased << 52) | fraction;
    if (finite.negative) {
      bits |= Long.MIN_VALUE;
    }
    return Double.longBitsToDouble(bits);
  }

  private static int floorLog2(BigInteger numerator, BigInteger denominator, int exponent) {
    int candidate = numerator.bitLength() - denominator.bitLength() + exponent;
    if (compareScaled(numerator, denominator, exponent - candidate) < 0) {
      candidate--;
    }
    return candidate;
  }

  private static int compareScaled(
      BigInteger numerator, BigInteger denominator, int binaryShift) {
    if (binaryShift >= 0) {
      return numerator.shiftLeft(binaryShift).compareTo(denominator);
    }
    return numerator.compareTo(denominator.shiftLeft(-binaryShift));
  }

  private static Rounded quotient(
      BigInteger numerator,
      BigInteger denominator,
      int binaryShift,
      boolean negative,
      RoundingMode mode) {
    BigInteger scaledNumerator = numerator;
    BigInteger scaledDenominator = denominator;
    if (binaryShift >= 0) {
      scaledNumerator = scaledNumerator.shiftLeft(binaryShift);
    } else {
      scaledDenominator = scaledDenominator.shiftLeft(-binaryShift);
    }
    BigInteger[] division = scaledNumerator.divideAndRemainder(scaledDenominator);
    boolean inexact = division[1].signum() != 0;
    if (inexact && increment(
        division[0], division[1], scaledDenominator, negative, mode)) {
      division[0] = division[0].add(BigInteger.ONE);
    }
    return new Rounded(division[0], inexact);
  }

  private static boolean increment(
      BigInteger quotient,
      BigInteger remainder,
      BigInteger denominator,
      boolean negative,
      RoundingMode mode) {
    switch (mode) {
      case TOWARD_ZERO:
        return false;
      case TOWARD_POSITIVE:
        return !negative;
      case TOWARD_NEGATIVE:
        return negative;
      case TIES_AWAY:
        return remainder.shiftLeft(1).compareTo(denominator) >= 0;
      case TIES_TO_EVEN:
        int comparison = remainder.shiftLeft(1).compareTo(denominator);
        return comparison > 0 || (comparison == 0 && quotient.testBit(0));
      default:
        throw new IllegalStateException();
    }
  }

  private static boolean incrementSqrt(
      BigInteger root,
      BigInteger radicand,
      boolean negative,
      RoundingMode mode) {
    switch (mode) {
      case TOWARD_ZERO:
      case TOWARD_NEGATIVE:
        return false;
      case TOWARD_POSITIVE:
        return !negative;
      case TIES_AWAY:
      case TIES_TO_EVEN:
        BigInteger twiceRootPlusOne = root.shiftLeft(1).add(BigInteger.ONE);
        return radicand.shiftLeft(2).compareTo(twiceRootPlusOne.pow(2)) > 0;
      default:
        throw new IllegalStateException();
    }
  }

  private static Binary128 overflow128(
      boolean negative, RoundingMode mode, StatusFlags status) {
    status.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
    boolean infinity = mode == RoundingMode.TIES_TO_EVEN
        || mode == RoundingMode.TIES_AWAY
        || (mode == RoundingMode.TOWARD_POSITIVE && !negative)
        || (mode == RoundingMode.TOWARD_NEGATIVE && negative);
    if (infinity) {
      return negative ? Binary128.NEGATIVE_INFINITY : Binary128.POSITIVE_INFINITY;
    }
    return negative ? Binary128.NEGATIVE_MAX : Binary128.POSITIVE_MAX;
  }

  private static double overflow64(
      boolean negative, RoundingMode mode, StatusFlags status) {
    status.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
    boolean infinity = mode == RoundingMode.TIES_TO_EVEN
        || mode == RoundingMode.TIES_AWAY
        || (mode == RoundingMode.TOWARD_POSITIVE && !negative)
        || (mode == RoundingMode.TOWARD_NEGATIVE && negative);
    if (infinity) {
      return negative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
    }
    return negative ? -Double.MAX_VALUE : Double.MAX_VALUE;
  }

  private static final class Rounded {
    final BigInteger value;
    final boolean inexact;

    Rounded(BigInteger value, boolean inexact) {
      this.value = value;
      this.inexact = inexact;
    }
  }
}
