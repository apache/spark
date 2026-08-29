/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

final class BidFma {
  private static final int BID64_EXPONENT_BIAS = 398;
  private static final int BID64_MAX_EXPONENT = 767;
  private static final long BID64_MAX_COEFFICIENT = 9_999_999_999_999_999L;
  private static final long BID64_MIN_NORMAL = 1_000_000_000_000_000L;
  private static final int BID128_EXPONENT_BIAS = 6176;
  private static final int BID128_MAX_EXPONENT = 12_287;
  private static final long BID128_MAX_COEFFICIENT_HIGH = 0x0001_ed09_bead_87c0L;
  private static final long BID128_MAX_COEFFICIENT_LOW = 0x378d_8e63_ffff_ffffL;
  private static final long BID128_MIN_NORMAL_HIGH = 0x0000_314d_c644_8d93L;
  private static final long BID128_MIN_NORMAL_LOW = 0x38c1_5b0a_0000_0000L;

  private BidFma() {
  }

  static long fma64(long x, long y, long z, RoundingMode mode, StatusFlags flags) {
    if (Bid64Raw.isNaN(x) || Bid64Raw.isNaN(y) || Bid64Raw.isNaN(z)) {
      if (Bid64Raw.isSignalingNaN(x)
          || Bid64Raw.isSignalingNaN(y)
          || Bid64Raw.isSignalingNaN(z)) {
        flags.raise(StatusFlags.INVALID);
      }
      long nan = Bid64Raw.isNaN(z) ? z : Bid64Raw.isNaN(x) ? x : y;
      return BidIntegral.canonicalizeNaN64(nan, new StatusFlags());
    }
    if ((Bid64Raw.isInf(x) && Bid64Raw.isZero(y))
        || (Bid64Raw.isInf(y) && Bid64Raw.isZero(x))) {
      flags.raise(StatusFlags.INVALID);
      return Bid64.MASK_NAN;
    }
    if (Bid64Raw.isInf(x) || Bid64Raw.isInf(y)) {
      boolean negative = Bid64Raw.isSigned(x) ^ Bid64Raw.isSigned(y);
      if (Bid64Raw.isInf(z) && Bid64Raw.isSigned(z) != negative) {
        flags.raise(StatusFlags.INVALID);
        return Bid64.MASK_NAN;
      }
      return (negative ? Bid64.MASK_SIGN : 0L) | Bid64.MASK_INFINITY;
    }
    if (Bid64Raw.isInf(z)) {
      return (z & Bid64.MASK_SIGN) | Bid64.MASK_INFINITY;
    }
    long fast = fma64Finite(x, y, z, mode, flags);
    if (fast != Long.MIN_VALUE) {
      return fast;
    }
    DecNum a = unpack64(x);
    DecNum b = unpack64(y);
    DecNum c = unpack64(z);
    a.multiply(b);
    boolean productNegative = a.isNegative();
    boolean addendNegative = c.isNegative();
    addSigned(a, c);
    if (a.isZero() && productNegative != addendNegative
        && mode == RoundingMode.TOWARD_NEGATIVE) {
      a.setNegative();
    }
    DecNum minNormal = DecNum.ofCoefficient(false, PowersOfTen.LONG[15], -398);
    boolean tiny = a.compareAbsolute(minNormal) < 0;
    StatusFlags local = new StatusFlags();
    long result = a.packBid64(mode, local);
    if (tiny && local.contains(StatusFlags.INEXACT)) {
      local.raise(StatusFlags.UNDERFLOW);
    }
    flags.raise(local.bits());
    return result;
  }

  static void fma128(
      long xh, long xl,
      long yh, long yl,
      long zh, long zl,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    if (isFinite128(xh) && isFinite128(yh) && isFinite128(zh)
        && fma128Finite(xh, xl, yh, yl, zh, zl, mode, flags, out)) {
      return;
    }
    Bid128 x = Bid128.fromRawBits(xh, xl);
    Bid128 y = Bid128.fromRawBits(yh, yl);
    Bid128 z = Bid128.fromRawBits(zh, zl);
    if (x.isNaN() || y.isNaN() || z.isNaN()) {
      if (x.isSignalingNaN() || y.isSignalingNaN() || z.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      Bid128 nan = z.isNaN() ? z : x.isNaN() ? x : y;
      BidIntegral.canonicalizeNaN128(
          nan.highBits(), nan.lowBits(), new StatusFlags(), out);
      return;
    }
    if (x.isInfinite() && y.isZero() || y.isInfinite() && x.isZero()) {
      flags.raise(StatusFlags.INVALID);
      DecNum.store128(Bid128.QUIET_NAN, out);
      return;
    }
    if (x.isInfinite() || y.isInfinite()) {
      boolean negative = x.isSigned() ^ y.isSigned();
      if (z.isInfinite() && z.isSigned() != negative) {
        flags.raise(StatusFlags.INVALID);
        DecNum.store128(Bid128.QUIET_NAN, out);
        return;
      }
      DecNum.store128(negative ? Bid128.NEGATIVE_INFINITY : Bid128.POSITIVE_INFINITY, out);
      return;
    }
    if (z.isInfinite()) {
      DecNum.store128(z.isSigned() ? Bid128.NEGATIVE_INFINITY : Bid128.POSITIVE_INFINITY, out);
      return;
    }
    DecNum a = unpack128(x);
    DecNum b = unpack128(y);
    DecNum c = unpack128(z);
    a.multiply(b);
    boolean productNegative = a.isNegative();
    boolean addendNegative = c.isNegative();
    addSigned(a, c);
    if (a.isZero() && productNegative != addendNegative
        && mode == RoundingMode.TOWARD_NEGATIVE) {
      a.setNegative();
    }
    DecNum minNormal = DecNum.ofUnsigned(PowersOfTen.pow10(33).high(),
        PowersOfTen.pow10(33).low());
    minNormal.shiftExp(-6176);
    boolean tiny = a.compareAbsolute(minNormal) < 0;
    StatusFlags local = new StatusFlags();
    a.packBid128(mode, local, out);
    if (tiny && local.contains(StatusFlags.INEXACT)) {
      local.raise(StatusFlags.UNDERFLOW);
    }
    flags.raise(local.bits());
  }

  private static long fma64Finite(
      long x, long y, long z, RoundingMode mode, StatusFlags flags) {
    long xCoefficient = Bid64.significandBits(x);
    long yCoefficient = Bid64.significandBits(y);
    long zCoefficient = Bid64.significandBits(z);
    if (xCoefficient == 0 || yCoefficient == 0 || zCoefficient == 0) {
      return Long.MIN_VALUE;
    }
    int productExponent = Bid64.biasedExponentBits(x)
        + Bid64.biasedExponentBits(y) - BID64_EXPONENT_BIAS;
    int zExponent = Bid64.biasedExponentBits(z);
    int exponent = Math.min(productExponent, zExponent);
    Wide product = Wide.multiply64(xCoefficient, yCoefficient);
    Wide addend = Wide.from64(zCoefficient);
    int productScale = productExponent - exponent;
    int addendScale = zExponent - exponent;
    if (product.decimalDigits() + productScale > 68
        || addend.decimalDigits() + addendScale > 68) {
      return Long.MIN_VALUE;
    }
    product.multiplyPower10(productScale);
    addend.multiplyPower10(addendScale);
    boolean productNegative = ((x ^ y) & Bid64.MASK_SIGN) != 0;
    boolean addendNegative = (z & Bid64.MASK_SIGN) != 0;
    boolean negative = combine(product, productNegative, addend, addendNegative, mode);
    return roundAndPack64(product, negative, exponent, mode, flags);
  }

  private static boolean fma128Finite(
      long xh, long xl,
      long yh, long yl,
      long zh, long zl,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    boolean xCanonical = Bid128.isCanonicalFinite(xh, xl);
    boolean yCanonical = Bid128.isCanonicalFinite(yh, yl);
    boolean zCanonical = Bid128.isCanonicalFinite(zh, zl);
    long xHigh = xCanonical ? xh & Bid128.MASK_COEFFICIENT : 0L;
    long xLow = xCanonical ? xl : 0L;
    long yHigh = yCanonical ? yh & Bid128.MASK_COEFFICIENT : 0L;
    long yLow = yCanonical ? yl : 0L;
    long zHigh = zCanonical ? zh & Bid128.MASK_COEFFICIENT : 0L;
    long zLow = zCanonical ? zl : 0L;
    if ((xHigh | xLow) == 0 || (yHigh | yLow) == 0 || (zHigh | zLow) == 0) {
      return false;
    }
    int productExponent = exponent128(xh) + exponent128(yh) - BID128_EXPONENT_BIAS;
    int zExponent = exponent128(zh);
    int exponent = Math.min(productExponent, zExponent);
    Wide product = Wide.multiply128(xHigh, xLow, yHigh, yLow);
    Wide addend = Wide.from128(zHigh, zLow);
    int productScale = productExponent - exponent;
    int addendScale = zExponent - exponent;
    if (product.decimalDigits() + productScale > 68
        || addend.decimalDigits() + addendScale > 68) {
      return false;
    }
    product.multiplyPower10(productScale);
    addend.multiplyPower10(addendScale);
    boolean productNegative = ((xh ^ yh) & Bid128.MASK_SIGN) != 0;
    boolean addendNegative = (zh & Bid128.MASK_SIGN) != 0;
    boolean negative = combine(product, productNegative, addend, addendNegative, mode);
    roundAndPack128(product, negative, exponent, mode, flags, out);
    return true;
  }

  private static boolean combine(
      Wide product,
      boolean productNegative,
      Wide addend,
      boolean addendNegative,
      RoundingMode mode) {
    if (productNegative == addendNegative) {
      product.add(addend);
      return productNegative;
    }
    int comparison = product.compareTo(addend);
    if (comparison == 0) {
      product.clear();
      return mode == RoundingMode.TOWARD_NEGATIVE;
    }
    if (comparison > 0) {
      product.subtract(addend);
      return productNegative;
    }
    addend.subtract(product);
    product.copyFrom(addend);
    return addendNegative;
  }

  private static long roundAndPack64(
      Wide value,
      boolean negative,
      int exponent,
      RoundingMode mode,
      StatusFlags flags) {
    if (value.isZero()) {
      int zeroExponent = Math.max(0, Math.min(BID64_MAX_EXPONENT, exponent));
      return Bid64.finiteRawBits(negative, zeroExponent, 0);
    }
    boolean tiny = isTiny(value, exponent, 15);
    int digits = value.decimalDigits();
    int discard = Math.max(0, digits - 16);
    discard = Math.max(discard, -exponent);
    boolean inexact = value.round(discard, digits, negative, mode);
    exponent += discard;
    long coefficient = value.low;
    if (coefficient == 10_000_000_000_000_000L) {
      coefficient = BID64_MIN_NORMAL;
      exponent++;
    }
    while (exponent > BID64_MAX_EXPONENT
        && coefficient <= BID64_MAX_COEFFICIENT / 10) {
      coefficient *= 10;
      exponent--;
    }
    if (exponent > BID64_MAX_EXPONENT) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
      return overflow64(negative, mode);
    }
    if (inexact) {
      flags.raise(StatusFlags.INEXACT);
      if (tiny) {
        flags.raise(StatusFlags.UNDERFLOW);
      }
    }
    return Bid64.finiteRawBits(negative, exponent, coefficient);
  }

  private static void roundAndPack128(
      Wide value,
      boolean negative,
      int exponent,
      RoundingMode mode,
      StatusFlags flags,
      long[] out) {
    if (value.isZero()) {
      int zeroExponent = Math.max(0, Math.min(BID128_MAX_EXPONENT, exponent));
      out[0] = (negative ? Bid128.MASK_SIGN : 0L) | ((long) zeroExponent << 49);
      out[1] = 0L;
      return;
    }
    boolean tiny = isTiny(value, exponent, 33);
    int digits = value.decimalDigits();
    int discard = Math.max(0, digits - 34);
    discard = Math.max(discard, -exponent);
    boolean inexact = value.round(discard, digits, negative, mode);
    exponent += discard;
    if (compare128(
        value.midLow,
        value.low,
        0x0001_ed09_bead_87c0L,
        0x378d_8e64_0000_0000L) == 0) {
      value.midLow = BID128_MIN_NORMAL_HIGH;
      value.low = BID128_MIN_NORMAL_LOW;
      exponent++;
    }
    while (exponent > BID128_MAX_EXPONENT
        && compare128(
            value.midLow,
            value.low,
            0x0000_314d_c644_8d93L,
            0x38c1_5b09_ffff_ffffL) <= 0) {
      value.multiplyByTen();
      exponent--;
    }
    if (exponent > BID128_MAX_EXPONENT) {
      flags.raise(StatusFlags.OVERFLOW | StatusFlags.INEXACT);
      overflow128(negative, mode, out);
      return;
    }
    if (inexact) {
      flags.raise(StatusFlags.INEXACT);
      if (tiny) {
        flags.raise(StatusFlags.UNDERFLOW);
      }
    }
    out[0] = (negative ? Bid128.MASK_SIGN : 0L)
        | ((long) exponent << 49) | value.midLow;
    out[1] = value.low;
  }

  private static boolean isTiny(Wide value, int exponent, int normalDigits) {
    if (exponent > 0) {
      return false;
    }
    int thresholdDigits = normalDigits - exponent;
    return thresholdDigits > 68
        || value.compareTo(Wide.POWERS_OF_TEN[thresholdDigits]) < 0;
  }

  private static long overflow64(boolean negative, RoundingMode mode) {
    boolean infinity = roundsToInfinity(negative, mode);
    if (infinity) {
      return (negative ? Bid64.MASK_SIGN : 0L) | Bid64.MASK_INFINITY;
    }
    return Bid64.finiteRawBits(
        negative, BID64_MAX_EXPONENT, BID64_MAX_COEFFICIENT);
  }

  private static void overflow128(
      boolean negative, RoundingMode mode, long[] out) {
    if (roundsToInfinity(negative, mode)) {
      out[0] = (negative ? Bid128.MASK_SIGN : 0L) | Bid128.MASK_INFINITY;
      out[1] = 0L;
      return;
    }
    out[0] = (negative ? Bid128.MASK_SIGN : 0L)
        | ((long) BID128_MAX_EXPONENT << 49) | BID128_MAX_COEFFICIENT_HIGH;
    out[1] = BID128_MAX_COEFFICIENT_LOW;
  }

  private static boolean roundsToInfinity(boolean negative, RoundingMode mode) {
    switch (mode) {
      case TIES_TO_EVEN:
      case TIES_AWAY:
        return true;
      case TOWARD_POSITIVE:
        return !negative;
      case TOWARD_NEGATIVE:
        return negative;
      case TOWARD_ZERO:
        return false;
      default:
        throw new AssertionError(mode);
    }
  }

  private static boolean isFinite128(long high) {
    return (high & Bid128.MASK_INFINITY) != Bid128.MASK_INFINITY;
  }

  private static int exponent128(long high) {
    return (int) ((high & Bid128.MASK_EXPONENT) >>> 49);
  }

  private static int compare128(
      long high, long low, long otherHigh, long otherLow) {
    int comparison = Long.compareUnsigned(high, otherHigh);
    return comparison != 0 ? comparison : Long.compareUnsigned(low, otherLow);
  }

  private static void addSigned(DecNum a, DecNum c) {
    int cmp = a.compareAbsolute(c);
    if (a.isNegative() == c.isNegative()) {
      a.addAbsolute(c);
      return;
    }
    if (cmp >= 0) {
      a.subtractAbsolute(c);
    } else {
      boolean negative = c.isNegative();
      c.subtractAbsolute(a);
      a.copyFrom(c);
      if (negative) {
        a.setNegative();
      }
    }
  }

  private static DecNum unpack64(long x) {
    return DecNum.ofCoefficient(
        Bid64Raw.isSigned(x),
        Bid64Raw.isZero(x) ? 0L : Bid64.significandBits(x),
        Bid64.biasedExponentBits(x) - 398);
  }

  private static DecNum unpack128(Bid128 x) {
    UInt128 coeff = x.coefficient();
    DecNum number = DecNum.ofUnsigned(coeff.high(), coeff.low());
    if (x.isSigned()) {
      number.setNegative();
    }
    number.shiftExp(x.biasedExponent() - 6176);
    return number;
  }

  /** Mutable unsigned 256-bit value for exact finite FMA intermediates. */
  private static final class Wide {
    private static final long WORD_MASK = 0xffff_ffffL;
    private static final int[] SMALL_POWERS_OF_TEN = {
      1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000,
      100_000_000, 1_000_000_000
    };
    private static final Wide[] POWERS_OF_TEN = powersOfTen();

    private long high;
    private long midHigh;
    private long midLow;
    private long low;

    private static Wide from64(long value) {
      Wide result = new Wide();
      result.low = value;
      return result;
    }

    private static Wide from128(long high, long low) {
      Wide result = new Wide();
      result.midLow = high;
      result.low = low;
      return result;
    }

    private static Wide multiply64(long left, long right) {
      Wide result = new Wide();
      result.low = left * right;
      result.midLow = UInt128.unsignedMultiplyHigh(left, right);
      return result;
    }

    private static Wide multiply128(
        long xHigh, long xLow, long yHigh, long yLow) {
      Wide result = new Wide();
      long p00High = UInt128.unsignedMultiplyHigh(xLow, yLow);
      long p01Low = xLow * yHigh;
      long p01High = UInt128.unsignedMultiplyHigh(xLow, yHigh);
      long p10Low = xHigh * yLow;
      long p10High = UInt128.unsignedMultiplyHigh(xHigh, yLow);
      long p11Low = xHigh * yHigh;
      long p11High = UInt128.unsignedMultiplyHigh(xHigh, yHigh);
      result.low = xLow * yLow;
      long sum = p00High + p01Low;
      long carry = Long.compareUnsigned(sum, p00High) < 0 ? 1 : 0;
      long next = sum + p10Low;
      carry += Long.compareUnsigned(next, sum) < 0 ? 1 : 0;
      result.midLow = next;
      sum = p01High + p10High;
      long highCarry = Long.compareUnsigned(sum, p01High) < 0 ? 1 : 0;
      next = sum + p11Low;
      highCarry += Long.compareUnsigned(next, sum) < 0 ? 1 : 0;
      sum = next + carry;
      highCarry += Long.compareUnsigned(sum, next) < 0 ? 1 : 0;
      result.midHigh = sum;
      result.high = p11High + highCarry;
      return result;
    }

    private void multiplyPower10(int power) {
      for (int i = 0; i < power; i++) {
        multiplyByTen();
      }
    }

    private void multiplyByTen() {
      long carry = UInt128.unsignedMultiplyHigh(low, 10);
      low *= 10;
      long next = midLow * 10 + carry;
      carry = UInt128.unsignedMultiplyHigh(midLow, 10)
          + (Long.compareUnsigned(next, carry) < 0 ? 1 : 0);
      midLow = next;
      next = midHigh * 10 + carry;
      carry = UInt128.unsignedMultiplyHigh(midHigh, 10)
          + (Long.compareUnsigned(next, carry) < 0 ? 1 : 0);
      midHigh = next;
      high = high * 10 + carry;
    }

    private void add(Wide other) {
      long nextLow = low + other.low;
      long carry = Long.compareUnsigned(nextLow, low) < 0 ? 1 : 0;
      long nextMidLow = midLow + other.midLow;
      long carry1 = Long.compareUnsigned(nextMidLow, midLow) < 0 ? 1 : 0;
      long sum = nextMidLow + carry;
      carry1 += Long.compareUnsigned(sum, nextMidLow) < 0 ? 1 : 0;
      nextMidLow = sum;
      long nextMidHigh = midHigh + other.midHigh;
      long carry2 = Long.compareUnsigned(nextMidHigh, midHigh) < 0 ? 1 : 0;
      sum = nextMidHigh + carry1;
      carry2 += Long.compareUnsigned(sum, nextMidHigh) < 0 ? 1 : 0;
      low = nextLow;
      midLow = nextMidLow;
      midHigh = sum;
      high = high + other.high + carry2;
    }

    private void subtract(Wide other) {
      long nextLow = low - other.low;
      long borrow = Long.compareUnsigned(low, other.low) < 0 ? 1 : 0;
      long nextMidLow = midLow - other.midLow;
      long borrow1 = Long.compareUnsigned(midLow, other.midLow) < 0 ? 1 : 0;
      long difference = nextMidLow - borrow;
      borrow1 += borrow != 0 && nextMidLow == 0 ? 1 : 0;
      nextMidLow = difference;
      long nextMidHigh = midHigh - other.midHigh;
      long borrow2 = Long.compareUnsigned(midHigh, other.midHigh) < 0 ? 1 : 0;
      difference = nextMidHigh - borrow1;
      borrow2 += Long.compareUnsigned(nextMidHigh, borrow1) < 0 ? 1 : 0;
      low = nextLow;
      midLow = nextMidLow;
      midHigh = difference;
      high = high - other.high - borrow2;
    }

    private boolean round(
        int discard,
        int valueDigits,
        boolean negative,
        RoundingMode mode) {
      if (discard == 0) {
        return false;
      }
      if (discard > valueDigits) {
        clear();
        if (mode == RoundingMode.TOWARD_POSITIVE && !negative
            || mode == RoundingMode.TOWARD_NEGATIVE && negative) {
          low = 1;
        }
        return true;
      }
      boolean sticky = false;
      int remaining = discard;
      while (remaining > 9) {
        sticky |= divideByInt(1_000_000_000) != 0;
        remaining -= 9;
      }
      int remainder = divideByInt(SMALL_POWERS_OF_TEN[remaining]);
      int leading = SMALL_POWERS_OF_TEN[remaining - 1];
      int roundDigit = remainder / leading;
      sticky |= remainder % leading != 0;
      boolean inexact = roundDigit != 0 || sticky;
      boolean increment;
      switch (mode) {
        case TIES_TO_EVEN:
          increment = roundDigit > 5
              || roundDigit == 5 && (sticky || (low & 1) != 0);
          break;
        case TIES_AWAY:
          increment = roundDigit >= 5;
          break;
        case TOWARD_POSITIVE:
          increment = !negative && inexact;
          break;
        case TOWARD_NEGATIVE:
          increment = negative && inexact;
          break;
        case TOWARD_ZERO:
          increment = false;
          break;
        default:
          throw new AssertionError(mode);
      }
      if (increment) {
        increment();
      }
      return inexact;
    }

    private int divideByInt(long divisor) {
      long remainder = 0;
      long dividend = high >>> 32;
      long nextHigh = dividend / divisor << 32;
      remainder = dividend % divisor;
      dividend = (remainder << 32) | (high & WORD_MASK);
      nextHigh |= dividend / divisor;
      remainder = dividend % divisor;
      dividend = (remainder << 32) | (midHigh >>> 32);
      long nextMidHigh = dividend / divisor << 32;
      remainder = dividend % divisor;
      dividend = (remainder << 32) | (midHigh & WORD_MASK);
      nextMidHigh |= dividend / divisor;
      remainder = dividend % divisor;
      dividend = (remainder << 32) | (midLow >>> 32);
      long nextMidLow = dividend / divisor << 32;
      remainder = dividend % divisor;
      dividend = (remainder << 32) | (midLow & WORD_MASK);
      nextMidLow |= dividend / divisor;
      remainder = dividend % divisor;
      dividend = (remainder << 32) | (low >>> 32);
      long nextLow = dividend / divisor << 32;
      remainder = dividend % divisor;
      dividend = (remainder << 32) | (low & WORD_MASK);
      nextLow |= dividend / divisor;
      remainder = dividend % divisor;
      high = nextHigh;
      midHigh = nextMidHigh;
      midLow = nextMidLow;
      low = nextLow;
      return (int) remainder;
    }

    private int decimalDigits() {
      int bits = bitLength();
      if (bits == 0) {
        return 1;
      }
      int digits = (((bits - 1) * 1233) >>> 12) + 1;
      if (compareTo(POWERS_OF_TEN[digits]) >= 0) {
        digits++;
      }
      return digits;
    }

    private int bitLength() {
      if (high != 0) {
        return 256 - Long.numberOfLeadingZeros(high);
      }
      if (midHigh != 0) {
        return 192 - Long.numberOfLeadingZeros(midHigh);
      }
      if (midLow != 0) {
        return 128 - Long.numberOfLeadingZeros(midLow);
      }
      return low == 0 ? 0 : 64 - Long.numberOfLeadingZeros(low);
    }

    private boolean isZero() {
      return (high | midHigh | midLow | low) == 0;
    }

    private int compareTo(Wide other) {
      int comparison = Long.compareUnsigned(high, other.high);
      if (comparison == 0) {
        comparison = Long.compareUnsigned(midHigh, other.midHigh);
      }
      if (comparison == 0) {
        comparison = Long.compareUnsigned(midLow, other.midLow);
      }
      return comparison != 0 ? comparison : Long.compareUnsigned(low, other.low);
    }

    private void increment() {
      low++;
      if (low != 0) {
        return;
      }
      midLow++;
      if (midLow != 0) {
        return;
      }
      midHigh++;
      if (midHigh == 0) {
        high++;
      }
    }

    private void clear() {
      high = 0;
      midHigh = 0;
      midLow = 0;
      low = 0;
    }

    private void copyFrom(Wide other) {
      high = other.high;
      midHigh = other.midHigh;
      midLow = other.midLow;
      low = other.low;
    }

    private static Wide[] powersOfTen() {
      Wide[] result = new Wide[70];
      Wide value = from64(1);
      for (int i = 0; i < result.length; i++) {
        Wide copy = new Wide();
        copy.copyFrom(value);
        result[i] = copy;
        value.multiplyByTen();
      }
      return result;
    }
  }
}
