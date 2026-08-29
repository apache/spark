/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

/**
 * Packed IEEE 754 binary128 (two {@code long}s, high then low).
 *
 * <p>Bit layout, big-endian in the packed high/low convention used by Intel
 * RDFP and intended for later {@code BidConvert}:
 * <pre>
 *   high[63]       sign
 *   high[62:48]    biased exponent (bias {@link #BIAS} = 16383)
 *   high[47:0]     fraction bits 111:64
 *   low[63:0]      fraction bits 63:0
 * </pre>
 * The implicit leading significand bit is 1 for normals and 0 for subnormals.
 * A 112-bit fraction is {@code (fractionHigh() &lt;&lt; 64) | fractionLow()}.
 * A 113-bit integer significand for normals is
 * {@code (1 &lt;&lt; 112) | fraction}.
 *
 * <p>Quiet NaNs have high bit 47 set ({@code 0x0000800000000000} in
 * {@code high}). Signaling NaNs have a nonzero payload with that bit clear.
 *
 * <p>This type is the packed seam for the bounded DPML engine used by this
 * libbid port. It is not a complete general-purpose binary128 Java type.
 * Arithmetic takes an explicit {@link RoundingMode} and {@link StatusFlags};
 * there is no process-global FPSR. BID convert wrappers stay in
 * {@code org.bidfp}.
 */
public final class Binary128 {
  public static final int BIAS = 16383;
  public static final int SIGNIFICAND_BITS = 112;
  public static final long MASK_SIGN = 0x8000_0000_0000_0000L;
  public static final long MASK_EXPONENT = 0x7fff_0000_0000_0000L;
  public static final long MASK_FRACTION_HIGH = 0x0000_ffff_ffff_ffffL;
  public static final long QUIET_NAN_BIT = 0x0000_8000_0000_0000L;

  public static final Binary128 ZERO = fromRawBits(0L, 0L);
  public static final Binary128 NEGATIVE_ZERO = fromRawBits(MASK_SIGN, 0L);
  public static final Binary128 ONE = fromRawBits(0x3fff_0000_0000_0000L, 0L);
  public static final Binary128 POSITIVE_INFINITY =
      fromRawBits(0x7fff_0000_0000_0000L, 0L);
  public static final Binary128 NEGATIVE_INFINITY =
      fromRawBits(0xffff_0000_0000_0000L, 0L);
  public static final Binary128 NAN = fromRawBits(QNAN_HIGH(), 0L);
  public static final Binary128 POSITIVE_MAX =
      fromRawBits(0x7ffe_ffff_ffff_ffffL, 0xffff_ffff_ffff_ffffL);
  public static final Binary128 NEGATIVE_MAX =
      fromRawBits(0xfffe_ffff_ffff_ffffL, 0xffff_ffff_ffff_ffffL);

  private static long QNAN_HIGH() {
    return 0x7fff_8000_0000_0000L;
  }

  private final long high;
  private final long low;

  private Binary128(long high, long low) {
    this.high = high;
    this.low = low;
  }

  public static Binary128 fromRawBits(long high, long low) {
    return new Binary128(high, low);
  }

  /**
   * Packs IEEE fields. {@code fractionHigh} is the top 48 fraction bits;
   * {@code fractionLow} is the low 64. Does not insert the implicit bit.
   */
  public static Binary128 fromFields(
      boolean sign, int biasedExponent, long fractionHigh, long fractionLow) {
    long high = (fractionHigh & MASK_FRACTION_HIGH)
        | (((long) biasedExponent & 0x7fffL) << 48);
    if (sign) {
      high |= MASK_SIGN;
    }
    return new Binary128(high, fractionLow);
  }

  public static Binary128 canonicalNaN(boolean sign) {
    long high = 0x7fff_8000_0000_0000L;
    if (sign) {
      high |= MASK_SIGN;
    }
    return new Binary128(high, 0L);
  }

  public static Binary128 fromBinary64(double value) {
    long bits = Double.doubleToRawLongBits(value);
    boolean sign = bits < 0L;
    int dexp = (int) ((bits >>> 52) & 0x7ffL);
    long dfrac = bits & 0x000f_ffff_ffff_ffffL;
    if (dexp == 0x7ff) {
      if (dfrac == 0L) {
        return sign ? NEGATIVE_INFINITY : POSITIVE_INFINITY;
      }
      long high = 0x7fff_0000_0000_0000L | (dfrac >>> 4);
      if ((dfrac & 0x0008_0000_0000_0000L) != 0L) {
        high |= QUIET_NAN_BIT;
      } else if ((high & MASK_FRACTION_HIGH) == 0L) {
        high |= QUIET_NAN_BIT;
      }
      if (sign) {
        high |= MASK_SIGN;
      }
      return new Binary128(high, dfrac << 60);
    }
    if (dexp == 0) {
      if (dfrac == 0L) {
        return sign ? NEGATIVE_ZERO : ZERO;
      }
      int clz = Long.numberOfLeadingZeros(dfrac);
      int shift = clz - 11;
      long sig = dfrac << shift;
      int unbiased = -1022 - shift;
      int biased = unbiased + BIAS;
      long fraction = sig & 0x000f_ffff_ffff_ffffL;
      return fromFields(sign, biased, fraction >>> 4, fraction << 60);
    }
    int biased = dexp - 1023 + BIAS;
    return fromFields(sign, biased, dfrac >>> 4, dfrac << 60);
  }

  public double toBinary64(RoundingMode mode, StatusFlags status) {
    Unpacked u = UxOps.unpack(this);
    if (u.isNaN()) {
      if (u.signaling) {
        status.raise(StatusFlags.INVALID);
      }
      long payload = (fractionHigh() << 4) | (fractionLow() >>> 60);
      payload |= 0x0008_0000_0000_0000L;
      long bits = 0x7ff0_0000_0000_0000L | payload;
      if (isSigned()) {
        bits |= MASK_SIGN;
      }
      return Double.longBitsToDouble(bits);
    }
    if (u.isInfinite()) {
      return u.sign != 0 ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
    }
    if (u.isZero()) {
      return u.sign != 0 ? -0.0 : 0.0;
    }
    if (isSubnormal()) {
      status.raise(StatusFlags.DENORMAL);
    }
    return IeeeRound.binary64(this, mode, status);
  }

  public long highBits() {
    return high;
  }

  public long lowBits() {
    return low;
  }

  public boolean isSigned() {
    return (high & MASK_SIGN) != 0L;
  }

  public int biasedExponent() {
    return (int) ((high & MASK_EXPONENT) >>> 48);
  }

  /** Top 48 bits of the 112-bit trailing fraction (no implicit bit). */
  public long fractionHigh() {
    return high & MASK_FRACTION_HIGH;
  }

  /** Low 64 bits of the 112-bit trailing fraction. */
  public long fractionLow() {
    return low;
  }

  /**
   * High 49 bits of the 113-bit integer significand. Bit 48 is the implicit
   * bit for normals and 0 for zeros/subnormals.
   */
  public long significandHigh() {
    long frac = fractionHigh();
    int exp = biasedExponent();
    if (exp == 0 || exp == 0x7fff) {
      return frac;
    }
    return frac | (1L << 48);
  }

  public long significandLow() {
    return low;
  }

  public boolean isNaN() {
    return biasedExponent() == 0x7fff
        && ((high & MASK_FRACTION_HIGH) != 0L || low != 0L);
  }

  public boolean isSignalingNaN() {
    return isNaN() && (high & QUIET_NAN_BIT) == 0L;
  }

  public boolean isQuietNaN() {
    return isNaN() && (high & QUIET_NAN_BIT) != 0L;
  }

  public boolean isInfinite() {
    return biasedExponent() == 0x7fff
        && (high & MASK_FRACTION_HIGH) == 0L
        && low == 0L;
  }

  public boolean isFinite() {
    return biasedExponent() != 0x7fff;
  }

  public boolean isZero() {
    return (high & ~MASK_SIGN) == 0L && low == 0L;
  }

  public boolean isSubnormal() {
    return biasedExponent() == 0 && !isZero();
  }

  public boolean isNormal() {
    int exp = biasedExponent();
    return exp != 0 && exp != 0x7fff;
  }

  public Binary128 abs() {
    return fromRawBits(high & ~MASK_SIGN, low);
  }

  public Binary128 negate() {
    return fromRawBits(high ^ MASK_SIGN, low);
  }

  public Binary128 add(Binary128 other, RoundingMode mode, StatusFlags status) {
    return UxOps.add(this, other, mode, status);
  }

  public Binary128 subtract(Binary128 other, RoundingMode mode, StatusFlags status) {
    return UxOps.sub(this, other, mode, status);
  }

  public Binary128 multiply(Binary128 other, RoundingMode mode, StatusFlags status) {
    return UxOps.mul(this, other, mode, status);
  }

  public Binary128 divide(Binary128 other, RoundingMode mode, StatusFlags status) {
    return UxOps.div(this, other, mode, status);
  }

  public Binary128 sqrt(RoundingMode mode, StatusFlags status) {
    return UxOps.sqrt(this, mode, status);
  }

  public int compare(Binary128 other, StatusFlags status) {
    return UxOps.compare(this, other, status);
  }

  public void store(long[] out) {
    out[0] = high;
    out[1] = low;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Binary128 value)) {
      return false;
    }
    return high == value.high && low == value.low;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(high) * 31 + Long.hashCode(low);
  }

  @Override
  public String toString() {
    return String.format("0x%016x%016x", high, low);
  }
}
