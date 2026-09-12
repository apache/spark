/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   * Neither the name of Intel Corporation nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.bidfp;

import java.math.BigDecimal;

import org.bidfp.binary128.Binary128;

/** IEEE 754 decimal128 value in Binary Integer Decimal (BID) encoding. */
public final class Bid128 implements Comparable<Bid128> {
  private static final int UNORDERED = 2;

  static final long MASK_SIGN = 0x8000_0000_0000_0000L;
  static final long MASK_STEERING_BITS = 0x6000_0000_0000_0000L;
  static final long MASK_EXPONENT = 0x7ffe_0000_0000_0000L;
  static final long MASK_COEFFICIENT = 0x0001_ffff_ffff_ffffL;
  static final long MASK_INFINITY = 0x7800_0000_0000_0000L;
  static final long MASK_NAN = 0x7c00_0000_0000_0000L;
  static final long MASK_SIGNALING_NAN = 0x7e00_0000_0000_0000L;

  private static final UInt128 MAX_COEFFICIENT =
      new UInt128(0x0001_ed09_bead_87c0L, 0x378d_8e63_ffff_ffffL);
  private static final UInt128 MAX_NAN_PAYLOAD =
      new UInt128(0x0000_314d_c644_8d93L, 0x38c1_5b09_ffff_ffffL);
  private static final long[][] POW10 = powersOfTenBits();
  private static final long[] POW10_LONG = powersOfTenLong();
  private static final ThreadLocal<long[]> DIVIDE_RESULT =
      ThreadLocal.withInitial(() -> new long[2]);

  public static final Bid128 POSITIVE_INFINITY = fromRawBits(MASK_INFINITY, 0);
  public static final Bid128 NEGATIVE_INFINITY =
      fromRawBits(MASK_SIGN | MASK_INFINITY, 0);
  public static final Bid128 QUIET_NAN = fromRawBits(MASK_NAN, 0);
  public static final Bid128 SIGNALING_NAN = fromRawBits(MASK_SIGNALING_NAN, 0);
  public static final Bid128 POSITIVE_ZERO = fromRawBits(0, 0);
  public static final Bid128 NEGATIVE_ZERO = fromRawBits(MASK_SIGN, 0);

  private final long high;
  private final long low;

  private Bid128(long high, long low) {
    this.high = high;
    this.low = low;
  }

  public static Bid128 fromRawBits(long high, long low) {
    return new Bid128(high, low);
  }

  public static Bid128 parseExact(String text) {
    String value = text.trim();
    boolean negative = value.startsWith("-");
    if (negative || value.startsWith("+")) {
      value = value.substring(1);
    }
    if (value.equalsIgnoreCase("Infinity") || value.equalsIgnoreCase("Inf")) {
      return negative ? NEGATIVE_INFINITY : POSITIVE_INFINITY;
    }
    if (value.equalsIgnoreCase("NaN")) {
      return fromRawBits((negative ? MASK_SIGN : 0) | MASK_NAN, 0);
    }
    if (value.equalsIgnoreCase("SNaN")) {
      return fromRawBits((negative ? MASK_SIGN : 0) | MASK_SIGNALING_NAN, 0);
    }

    int ePosition = Math.max(value.indexOf('E'), value.indexOf('e'));
    long explicitExponent = ePosition < 0
        ? 0
        : Long.parseLong(value.substring(ePosition + 1));
    String mantissa = ePosition < 0 ? value : value.substring(0, ePosition);
    int point = mantissa.indexOf('.');
    if (point != mantissa.lastIndexOf('.')) {
      throw new NumberFormatException("multiple decimal points");
    }
    int fractionalDigits = point < 0 ? 0 : mantissa.length() - point - 1;
    String digits = point < 0
        ? mantissa
        : mantissa.substring(0, point) + mantissa.substring(point + 1);
    if (digits.isEmpty() || !digits.chars().allMatch(c -> c >= '0' && c <= '9')) {
      throw new NumberFormatException("invalid decimal significand");
    }
    int firstNonZero = 0;
    while (firstNonZero < digits.length() && digits.charAt(firstNonZero) == '0') {
      firstNonZero++;
    }
    digits = firstNonZero == digits.length() ? "0" : digits.substring(firstNonZero);
    long exponent = Math.subtractExact(explicitExponent, fractionalDigits);
    while (digits.length() > 34 && digits.endsWith("0")) {
      digits = digits.substring(0, digits.length() - 1);
      exponent = Math.incrementExact(exponent);
    }
    if (digits.length() > 34) {
      throw new ArithmeticException("value is not exactly representable as BID128");
    }
    if (digits.equals("0")) {
      exponent = Math.max(-6176, Math.min(6111, exponent));
    }
    while (exponent > 6111 && digits.length() < 34) {
      digits += "0";
      exponent--;
    }
    while (exponent < -6176 && digits.length() > 1 && digits.endsWith("0")) {
      digits = digits.substring(0, digits.length() - 1);
      exponent++;
    }
    UInt128 coefficient = UInt128.ZERO;
    for (int i = 0; i < digits.length(); i++) {
      coefficient = coefficient.shiftLeft(3)
          .add(coefficient.shiftLeft(1))
          .add(digits.charAt(i) - '0');
    }
    long biasedLong = Math.addExact(exponent, 6176);
    int biasedExponent = (int) biasedLong;
    if (biasedLong < 0 || biasedLong > 12_287) {
      throw new ArithmeticException("value is outside the BID128 exponent range");
    }
    return finite(negative, biasedExponent, coefficient.high(), coefficient.low());
  }

  public static Bid128 parse(String text, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.fromString(text, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  /**
   * Converts a finite Java decimal value using the requested IEEE rounding mode.
   *
   * <p>The input's decimal scale is preserved when it fits the decimal128
   * exponent and precision. Arithmetic remains independent of
   * {@link BigDecimal}; this method is an interoperability boundary.
   */
  public static Bid128 fromBigDecimal(
      BigDecimal value, RoundingMode mode, StatusFlags flags) {
    return parse(value.toString(), mode, flags);
  }

  /**
   * Converts a Java decimal value exactly, preserving its scale when representable.
   *
   * @throws ArithmeticException if the value is not exactly representable as decimal128
   */
  public static Bid128 fromBigDecimalExact(BigDecimal value) {
    return parseExact(value.toString());
  }

  public static Bid128 fromLong(long value, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.fromInt64(value, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public static Bid128 fromDouble(double value, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.fromBinary64(value, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public static Bid128 fromFloat(float value, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.fromBinary32(value, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public static Bid128 finite(
      boolean negative, int biasedExponent, long coefficientHigh, long coefficientLow) {
    if (biasedExponent < 0 || biasedExponent > 12_287) {
      throw new IllegalArgumentException("biased exponent must be in [0, 12287]");
    }
    if (compareCoefficient(
        coefficientHigh,
        coefficientLow,
        MAX_COEFFICIENT.high(),
        MAX_COEFFICIENT.low()) > 0) {
      throw new IllegalArgumentException("coefficient must be less than 10^34");
    }
    return rawFinite(negative, biasedExponent, coefficientHigh, coefficientLow);
  }

  static Bid128 rawFinite(
      boolean negative, int biasedExponent, long coefficientHigh, long coefficientLow) {
    long sign = negative ? MASK_SIGN : 0;
    long high = sign | ((long) biasedExponent << 49) | coefficientHigh;
    return fromRawBits(high, coefficientLow);
  }

  static boolean isCanonicalFinite(long high, long low) {
    if ((high & MASK_STEERING_BITS) == MASK_STEERING_BITS) {
      return false;
    }
    return compareCoefficient(
        high & MASK_COEFFICIENT,
        low,
        MAX_COEFFICIENT.high(),
        MAX_COEFFICIENT.low()) <= 0;
  }

  private static int compareCoefficient(
      long high, long low, long otherHigh, long otherLow) {
    int comparison = Long.compareUnsigned(high, otherHigh);
    return comparison != 0 ? comparison : Long.compareUnsigned(low, otherLow);
  }

  public long highBits() {
    return high;
  }

  public long lowBits() {
    return low;
  }

  public long toLong(RoundingMode mode, StatusFlags flags) {
    StatusFlags conversionFlags = new StatusFlags();
    long result = Bid128Raw.toInt64(high, low, mode, conversionFlags, true);
    flags.raise(conversionFlags.bits());
    if (conversionFlags.contains(StatusFlags.INVALID)) {
      throw new ArithmeticException("BID128 value is outside the long range");
    }
    return result;
  }

  public double toDouble(RoundingMode mode, StatusFlags flags) {
    return Bid128Raw.toBinary64(high, low, mode, flags);
  }

  public float toFloat(RoundingMode mode, StatusFlags flags) {
    return Bid128Raw.toBinary32(high, low, mode, flags);
  }

  public Binary128 toBinary128(RoundingMode mode, StatusFlags flags) {
    long[] bits128 = new long[2];
    Bid128Raw.toBinary128(high, low, mode, flags, bits128);
    return Binary128.fromRawBits(bits128[0], bits128[1]);
  }

  public static Bid128 fromBinary128(
      Binary128 value, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.fromBinary128(value.highBits(), value.lowBits(), mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid64 toBid64(RoundingMode mode, StatusFlags flags) {
    return Bid64.fromRawBits(Bid128Raw.toBid64(high, low, mode, flags));
  }

  /** Exact coefficient-and-exponent text that preserves the value's quantum. */
  public String toCanonicalString() {
    String sign = isSigned() ? "-" : "";
    if (isNaN()) {
      return sign + (isSignalingNaN() ? "SNaN" : "NaN");
    }
    if (isInfinite()) {
      return sign + "Infinity";
    }
    int exponent = biasedExponent() - 6176;
    String exponentText = exponent >= 0 ? "+" + exponent : Integer.toString(exponent);
    return sign + coefficient().toDecimalString() + "E" + exponentText;
  }

  /**
   * Returns the exact finite value as a Java decimal, preserving its quantum as scale.
   *
   * <p>{@link BigDecimal} has no signed zero, infinity, or NaN. Consequently,
   * signed zero loses its sign and non-finite values are rejected.
   *
   * @throws ArithmeticException if this value is infinite or NaN
   */
  public BigDecimal toBigDecimal() {
    if (!isFinite()) {
      throw new ArithmeticException("non-finite BID128 has no BigDecimal representation");
    }
    return new BigDecimal(toCanonicalString());
  }

  public boolean isSigned() {
    return (high & MASK_SIGN) != 0;
  }

  public boolean isFinite() {
    return (high & MASK_INFINITY) != MASK_INFINITY;
  }

  public boolean isInfinite() {
    return (high & MASK_INFINITY) == MASK_INFINITY && (high & MASK_NAN) != MASK_NAN;
  }

  public boolean isNaN() {
    return (high & MASK_NAN) == MASK_NAN;
  }

  public boolean isSignalingNaN() {
    return (high & MASK_SIGNALING_NAN) == MASK_SIGNALING_NAN;
  }

  public boolean isCanonical() {
    if (isNaN()) {
      if ((high & 0x01ff_c000_0000_0000L) != 0) {
        return false;
      }
      UInt128 payload = new UInt128(high & 0x0000_3fff_ffff_ffffL, low);
      return payload.compareTo(MAX_NAN_PAYLOAD) <= 0;
    }
    if ((high & MASK_INFINITY) == MASK_INFINITY) {
      return (high & 0x03ff_ffff_ffff_ffffL) == 0 && low == 0;
    }
    return isCanonicalFinite(high, low);
  }

  public boolean isZero() {
    return isFinite() && (!isCanonical() || coefficient().equals(UInt128.ZERO));
  }

  public boolean isNormal() {
    return classify().isNormal();
  }

  public boolean isSubnormal() {
    return classify().isSubnormal();
  }

  public Bid128 negate() {
    return fromRawBits(high ^ MASK_SIGN, low);
  }

  public Bid128 abs() {
    return fromRawBits(high & ~MASK_SIGN, low);
  }

  public Bid128 copySign(Bid128 signSource) {
    return fromRawBits((high & ~MASK_SIGN) | (signSource.high & MASK_SIGN), low);
  }

  public boolean sameQuantum(Bid128 other) {
    if (isNaN() || other.isNaN()) {
      return isNaN() && other.isNaN();
    }
    if (isInfinite() || other.isInfinite()) {
      return isInfinite() && other.isInfinite();
    }
    return biasedExponent() == other.biasedExponent();
  }

  public boolean totalOrder(Bid128 other) {
    return totalOrderValues(this, other);
  }

  public boolean totalOrderMag(Bid128 other) {
    return totalOrderValues(abs(), other.abs());
  }

  public Bid128 add(Bid128 other, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.add(high, low, other.high, other.low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 subtract(Bid128 other, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.sub(high, low, other.high, other.low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 multiply(Bid128 other, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.mul(high, low, other.high, other.low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 divide(Bid128 other, RoundingMode mode, StatusFlags flags) {
    long[] result = DIVIDE_RESULT.get();
    Bid128Raw.div(high, low, other.high, other.low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 sqrt(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.sqrt(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 cbrt(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.cbrt(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 exp(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.exp(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 expm1(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.expm1(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 exp2(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.exp2(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 exp10(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.exp10(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 log(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.log(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 log10(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.log10(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 log2(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.log2(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 log1p(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.log1p(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 pow(Bid128 y, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.pow(high, low, y.high, y.low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 hypot(Bid128 y, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.hypot(high, low, y.high, y.low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 sin(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.sin(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 cos(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.cos(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 tan(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.tan(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 asin(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.asin(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 acos(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.acos(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 atan(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.atan(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  /**
   * Two-argument arctangent. This value is y; {@code x} is the x argument.
   */
  public Bid128 atan2(Bid128 x, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.atan2(high, low, x.high, x.low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 sinh(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.sinh(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 cosh(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.cosh(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 tanh(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.tanh(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 asinh(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.asinh(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 acosh(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.acosh(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 atanh(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.atanh(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 erf(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.erf(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 erfc(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.erfc(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 tgamma(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.tgamma(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 lgamma(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.lgamma(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 fma(Bid128 y, Bid128 z, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.fma(
        high, low, y.high, y.low, z.high, z.low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 quantize(Bid128 exponent, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.quantize(
        high, low, exponent.high, exponent.low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 remainder(Bid128 other, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.rem(high, low, other.high, other.low, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 nextUp(StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.nextUp(high, low, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 nextDown(StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.nextDown(high, low, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 roundIntegral(RoundingMode mode, boolean exact, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.roundIntegral(high, low, mode, flags, exact, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 nearbyInt(RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.nearbyint(high, low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 scaleByPowerOfTen(
      int n, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.scalbn(high, low, n, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 fmod(Bid128 other, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.fmod(high, low, other.high, other.low, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 nextAfter(Bid128 target, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.nextAfter(high, low, target.high, target.low, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 minNum(Bid128 other, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.minnum(high, low, other.high, other.low, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 maxNum(Bid128 other, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.maxnum(high, low, other.high, other.low, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 minNumMagnitude(Bid128 other, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.minnumMag(high, low, other.high, other.low, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 maxNumMagnitude(Bid128 other, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.maxnumMag(high, low, other.high, other.low, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public Bid128 positiveDifference(
      Bid128 other, RoundingMode mode, StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.fdim(high, low, other.high, other.low, mode, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public int quantumExponent(StatusFlags flags) {
    return Bid128Raw.quantexp(high, low, flags);
  }

  public Bid128 quantum() {
    long[] result = new long[2];
    Bid128Raw.quantum(high, low, result);
    return fromRawBits(result[0], result[1]);
  }

  public int ilogb(StatusFlags flags) {
    return Bid128Raw.ilogb(high, low, flags);
  }

  public Bid128 logb(StatusFlags flags) {
    long[] result = new long[2];
    Bid128Raw.logb(high, low, flags, result);
    return fromRawBits(result[0], result[1]);
  }

  public static Bid128 copy(Bid128 x) {
    return x;
  }

  public boolean signalingLess(Bid128 other, StatusFlags flags) {
    return compare(other, flags, true) == -1;
  }

  public boolean signalingEqual(Bid128 other, StatusFlags flags) {
    return compare(other, flags, true) == 0;
  }

  public boolean signalingNotEqual(Bid128 other, StatusFlags flags) {
    return compare(other, flags, true) != 0;
  }

  public boolean signalingLessEqual(Bid128 other, StatusFlags flags) {
    int comparison = compare(other, flags, true);
    return comparison == -1 || comparison == 0;
  }

  public boolean signalingGreater(Bid128 other, StatusFlags flags) {
    return compare(other, flags, true) == 1;
  }

  public boolean signalingGreaterEqual(Bid128 other, StatusFlags flags) {
    int comparison = compare(other, flags, true);
    return comparison == 0 || comparison == 1;
  }

  public boolean signalingOrdered(Bid128 other, StatusFlags flags) {
    return compare(other, flags, true) != UNORDERED;
  }

  public boolean signalingUnordered(Bid128 other, StatusFlags flags) {
    return compare(other, flags, true) == UNORDERED;
  }

  public boolean signalingGreaterUnordered(Bid128 other, StatusFlags flags) {
    int comparison = compare(other, flags, true);
    return comparison == 1 || comparison == UNORDERED;
  }

  public boolean signalingLessUnordered(Bid128 other, StatusFlags flags) {
    int comparison = compare(other, flags, true);
    return comparison == -1 || comparison == UNORDERED;
  }

  public boolean signalingNotGreater(Bid128 other, StatusFlags flags) {
    return compare(other, flags, true) != 1;
  }

  public boolean signalingNotLess(Bid128 other, StatusFlags flags) {
    return compare(other, flags, true) != -1;
  }

  public static Bid128 nan() {
    return QUIET_NAN;
  }

  public static Bid128 inf() {
    return POSITIVE_INFINITY;
  }

  public static int radix() {
    return 10;
  }

  public boolean quietEqual(Bid128 other, StatusFlags flags) {
    return compare(other, flags, false) == 0;
  }

  public boolean quietNotEqual(Bid128 other, StatusFlags flags) {
    return compare(other, flags, false) != 0;
  }

  public boolean quietLess(Bid128 other, StatusFlags flags) {
    return compare(other, flags, false) == -1;
  }

  public boolean quietLessEqual(Bid128 other, StatusFlags flags) {
    int comparison = compare(other, flags, false);
    return comparison == -1 || comparison == 0;
  }

  public boolean quietGreater(Bid128 other, StatusFlags flags) {
    return compare(other, flags, false) == 1;
  }

  public boolean quietGreaterEqual(Bid128 other, StatusFlags flags) {
    int comparison = compare(other, flags, false);
    return comparison == 0 || comparison == 1;
  }

  public boolean quietOrdered(Bid128 other, StatusFlags flags) {
    return compare(other, flags, false) != UNORDERED;
  }

  public boolean quietUnordered(Bid128 other, StatusFlags flags) {
    return compare(other, flags, false) == UNORDERED;
  }

  public boolean quietGreaterUnordered(Bid128 other, StatusFlags flags) {
    int comparison = compare(other, flags, false);
    return comparison == 1 || comparison == UNORDERED;
  }

  public boolean quietLessUnordered(Bid128 other, StatusFlags flags) {
    int comparison = compare(other, flags, false);
    return comparison == -1 || comparison == UNORDERED;
  }

  public boolean quietNotGreater(Bid128 other, StatusFlags flags) {
    return compare(other, flags, false) != 1;
  }

  public boolean quietNotLess(Bid128 other, StatusFlags flags) {
    return compare(other, flags, false) != -1;
  }

  private int compare(Bid128 other, StatusFlags flags, boolean signaling) {
    if (isNaN() || other.isNaN()) {
      if (signaling || isSignalingNaN() || other.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      return UNORDERED;
    }
    return compareNumeric(this, other);
  }

  public DecimalClass classify() {
    if (isNaN()) {
      return isSignalingNaN() ? DecimalClass.SIGNALING_NAN : DecimalClass.QUIET_NAN;
    }
    if (isInfinite()) {
      return isSigned() ? DecimalClass.NEGATIVE_INFINITY : DecimalClass.POSITIVE_INFINITY;
    }
    UInt128 coefficient = coefficient();
    if (!isCanonical() || coefficient.equals(UInt128.ZERO)) {
      return isSigned() ? DecimalClass.NEGATIVE_ZERO : DecimalClass.POSITIVE_ZERO;
    }
    int digits = decimalDigits(coefficient);
    boolean subnormal = biasedExponent() + digits <= 33;
    if (subnormal) {
      return isSigned() ? DecimalClass.NEGATIVE_SUBNORMAL : DecimalClass.POSITIVE_SUBNORMAL;
    }
    return isSigned() ? DecimalClass.NEGATIVE_NORMAL : DecimalClass.POSITIVE_NORMAL;
  }

  int biasedExponent() {
    if ((high & 0x6000_0000_0000_0000L) == 0x6000_0000_0000_0000L
        && isFinite()) {
      return (int) ((high >>> 47) & 0x3fffL);
    }
    return (int) ((high & MASK_EXPONENT) >>> 49);
  }

  UInt128 coefficient() {
    if (isFinite() && !isCanonicalFinite(high, low)) {
      return UInt128.ZERO;
    }
    return new UInt128(high & MASK_COEFFICIENT, low);
  }

  private static int decimalDigits(UInt128 value) {
    return decimalDigits(value.high(), value.low());
  }

  private static int compareNumeric(Bid128 x, Bid128 y) {
    long highX = x.high;
    long lowX = x.low;
    long highY = y.high;
    long lowY = y.low;
    if (highX == highY && lowX == lowY) {
      return 0;
    }
    boolean signedX = (highX & MASK_SIGN) != 0;
    boolean signedY = (highY & MASK_SIGN) != 0;
    boolean infiniteX =
        (highX & MASK_INFINITY) == MASK_INFINITY && (highX & MASK_NAN) != MASK_NAN;
    boolean infiniteY =
        (highY & MASK_INFINITY) == MASK_INFINITY && (highY & MASK_NAN) != MASK_NAN;
    if (infiniteX) {
      if (infiniteY) {
        return signedX == signedY ? 0 : (signedX ? -1 : 1);
      }
      return signedX ? -1 : 1;
    }
    if (infiniteY) {
      return signedY ? 1 : -1;
    }

    long coefficientHighX = highX & MASK_COEFFICIENT;
    long coefficientHighY = highY & MASK_COEFFICIENT;
    boolean canonicalX = isCanonicalFinite(highX, lowX);
    boolean canonicalY = isCanonicalFinite(highY, lowY);
    boolean zeroX = !canonicalX || (coefficientHighX | lowX) == 0;
    boolean zeroY = !canonicalY || (coefficientHighY | lowY) == 0;
    if (zeroX && zeroY) {
      return 0;
    }
    if (zeroX) {
      return signedY ? 1 : -1;
    }
    if (zeroY) {
      return signedX ? -1 : 1;
    }
    if (signedX != signedY) {
      return signedX ? -1 : 1;
    }
    int exponentX = (int) ((highX & MASK_EXPONENT) >>> 49);
    int exponentY = (int) ((highY & MASK_EXPONENT) >>> 49);
    int coefficientComparison =
        compareCoefficient(coefficientHighX, lowX, coefficientHighY, lowY);
    int magnitude;
    if (coefficientComparison > 0 && exponentX >= exponentY
        || coefficientComparison < 0 && exponentX <= exponentY) {
      magnitude = coefficientComparison;
    } else if (coefficientComparison == 0) {
      magnitude = Integer.compare(exponentX, exponentY);
    } else {
      magnitude = compareMagnitude(
          coefficientHighX,
          lowX,
          exponentX,
          coefficientHighY,
          lowY,
          exponentY);
    }
    return signedX ? -magnitude : magnitude;
  }

  private static boolean totalOrderValues(Bid128 x, Bid128 y) {
    if (x.isNaN()) {
      return totalOrderLeftNaN(x, y);
    }
    if (y.isNaN()) {
      return !y.isSigned();
    }
    if (x.equals(y)) {
      return true;
    }
    if (x.isSigned() != y.isSigned()) {
      return x.isSigned();
    }
    if (x.isInfinite()) {
      return x.isSigned() || y.isInfinite();
    }
    if (y.isInfinite()) {
      return !y.isSigned();
    }
    int numeric = compareNumeric(x, y);
    if (numeric != 0) {
      return numeric < 0;
    }
    int exponentComparison = Integer.compare(x.biasedExponent(), y.biasedExponent());
    if (exponentComparison == 0) {
      return true;
    }
    return x.isSigned() ? exponentComparison > 0 : exponentComparison < 0;
  }

  private static boolean totalOrderLeftNaN(Bid128 x, Bid128 y) {
    if (x.isSigned()) {
      if (!y.isNaN() || !y.isSigned()) {
        return true;
      }
      if (x.isSignalingNaN() != y.isSignalingNaN()) {
        return y.isSignalingNaN();
      }
      return canonicalNaNPayload(x).compareTo(canonicalNaNPayload(y)) >= 0;
    }
    if (!y.isNaN() || y.isSigned()) {
      return false;
    }
    if (x.isSignalingNaN() != y.isSignalingNaN()) {
      return x.isSignalingNaN();
    }
    return canonicalNaNPayload(x).compareTo(canonicalNaNPayload(y)) <= 0;
  }

  private static UInt128 canonicalNaNPayload(Bid128 value) {
    UInt128 payload =
        new UInt128(value.high & 0x0000_3fff_ffff_ffffL, value.low);
    return payload.compareTo(MAX_NAN_PAYLOAD) <= 0 ? payload : UInt128.ZERO;
  }

  private static int compareMagnitude(
      long highX,
      long lowX,
      int exponentX,
      long highY,
      long lowY,
      int exponentY) {
    int digitsX = decimalDigits(highX, lowX);
    int digitsY = decimalDigits(highY, lowY);
    int adjustedX = digitsX + exponentX;
    int adjustedY = digitsY + exponentY;
    if (adjustedX != adjustedY) {
      return Integer.compare(adjustedX, adjustedY);
    }
    if (digitsX < digitsY) {
      return compareScaled(highX, lowX, digitsY - digitsX, highY, lowY);
    }
    if (digitsY < digitsX) {
      return -compareScaled(highY, lowY, digitsX - digitsY, highX, lowX);
    }
    return compareCoefficient(highX, lowX, highY, lowY);
  }

  private static int decimalDigits(long high, long low) {
    int bitLength = high == 0
        ? 64 - Long.numberOfLeadingZeros(low)
        : 128 - Long.numberOfLeadingZeros(high);
    int digits = bitLength * 1233 >>> 12;
    if (compareCoefficient(high, low, POW10[0][digits], POW10[1][digits]) >= 0) {
      digits++;
    }
    return digits;
  }

  private static int compareScaled(
      long high, long low, int scale, long otherHigh, long otherLow) {
    while (scale != 0) {
      int step = Math.min(scale, 18);
      long factor = POW10_LONG[step];
      high = high * factor + unsignedMultiplyHigh(low, factor);
      low *= factor;
      scale -= step;
    }
    return compareCoefficient(high, low, otherHigh, otherLow);
  }

  private static long unsignedMultiplyHigh(long left, long right) {
    long high = Math.multiplyHigh(left, right);
    if (left < 0) {
      high += right;
    }
    return high;
  }

  private static long[][] powersOfTenBits() {
    long[][] result = new long[2][35];
    long high = 0;
    long low = 1;
    for (int exponent = 0; exponent < result[0].length; exponent++) {
      result[0][exponent] = high;
      result[1][exponent] = low;
      high = high * 10 + unsignedMultiplyHigh(low, 10);
      low *= 10;
    }
    return result;
  }

  private static long[] powersOfTenLong() {
    long[] result = new long[19];
    result[0] = 1;
    for (int exponent = 1; exponent < result.length; exponent++) {
      result[exponent] = result[exponent - 1] * 10;
    }
    return result;
  }

  /**
   * Orders all encodings using IEEE 754 {@code totalOrder}.
   *
   * <p>Unlike the quiet comparison methods, this ordering includes NaNs,
   * signed zeros, and distinct cohorts and is consistent with bitwise
   * {@link #equals(Object)}. It is not Spark SQL ordering; SQL callers must
   * use {@link DecFloatAdapters#sqlCompare128(long, long, long, long)}.
   */
  @Override
  public int compareTo(Bid128 other) {
    if (high == other.high && low == other.low) {
      return 0;
    }
    boolean before = totalOrder(other);
    boolean after = other.totalOrder(this);
    if (before != after) {
      return before ? -1 : 1;
    }
    int highComparison = Long.compareUnsigned(high, other.high);
    return highComparison != 0 ? highComparison : Long.compareUnsigned(low, other.low);
  }

  @Override
  public boolean equals(Object other) {
    return this == other
        || other instanceof Bid128
        && high == ((Bid128) other).high
        && low == ((Bid128) other).low;
  }

  @Override
  public int hashCode() {
    return 31 * Long.hashCode(high) + Long.hashCode(low);
  }

  @Override
  public String toString() {
    return String.format("Bid128[0x%016x%016x,%s]", high, low, classify());
  }
}
