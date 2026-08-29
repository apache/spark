/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

import java.math.BigDecimal;

import org.bidfp.binary128.Binary128;

/**
 * An IEEE 754 decimal64 value represented in Binary Integer Decimal (BID) encoding.
 *
 * <p>Classification, comparisons, conversions, and add/subtract/multiply/divide
 * are checked against the Intel RDFP {@code readtest.in} test vectors.
 */
public final class Bid64 implements Comparable<Bid64> {
  private static final int UNORDERED = 2;

  static final long MASK_SIGN = 0x8000_0000_0000_0000L;
  static final long MASK_STEERING_BITS = 0x6000_0000_0000_0000L;
  static final long MASK_BINARY_EXPONENT1 = 0x7fe0_0000_0000_0000L;
  static final long MASK_BINARY_SIGNIFICAND1 = 0x001f_ffff_ffff_ffffL;
  static final long MASK_BINARY_EXPONENT2 = 0x1ff8_0000_0000_0000L;
  static final long MASK_BINARY_SIGNIFICAND2 = 0x0007_ffff_ffff_ffffL;
  static final long MASK_BINARY_OR2 = 0x0020_0000_0000_0000L;
  static final long MASK_INFINITY = 0x7800_0000_0000_0000L;
  static final long MASK_NAN = 0x7c00_0000_0000_0000L;
  static final long MASK_SIGNALING_NAN = 0x7e00_0000_0000_0000L;

  private static final long MAX_SIGNIFICAND = 9_999_999_999_999_999L;
  private static final long MAX_NAN_PAYLOAD = 999_999_999_999_999L;
  private static final long MIN_NORMAL_SIGNIFICAND = 1_000_000_000_000_000L;
  private static final long[] POW10 = {
    1L,
    10L,
    100L,
    1_000L,
    10_000L,
    100_000L,
    1_000_000L,
    10_000_000L,
    100_000_000L,
    1_000_000_000L,
    10_000_000_000L,
    100_000_000_000L,
    1_000_000_000_000L,
    10_000_000_000_000L,
    100_000_000_000_000L,
    1_000_000_000_000_000L
  };

  public static final Bid64 POSITIVE_INFINITY = fromRawBits(MASK_INFINITY);
  public static final Bid64 NEGATIVE_INFINITY = fromRawBits(MASK_SIGN | MASK_INFINITY);
  public static final Bid64 QUIET_NAN = fromRawBits(MASK_NAN);
  public static final Bid64 SIGNALING_NAN = fromRawBits(MASK_SIGNALING_NAN);
  public static final Bid64 POSITIVE_ZERO = fromRawBits(0L);
  public static final Bid64 NEGATIVE_ZERO = fromRawBits(MASK_SIGN);

  private final long bits;

  private Bid64(long bits) {
    this.bits = bits;
  }

  public static Bid64 fromRawBits(long bits) {
    return new Bid64(bits);
  }

  /**
   * Packs a finite canonical BID64 value.
   *
   * @param negative whether the result has a negative sign
   * @param biasedExponent exponent in the encoded range [0, 767]
   * @param significand coefficient in the range [0, 10^16 - 1]
   */
  public static Bid64 finite(boolean negative, int biasedExponent, long significand) {
    if (biasedExponent < 0 || biasedExponent > 767) {
      throw new IllegalArgumentException("biased exponent must be in [0, 767]");
    }
    if (significand < 0 || significand > MAX_SIGNIFICAND) {
      throw new IllegalArgumentException("significand must be in [0, 10^16 - 1]");
    }

    return fromRawBits(finiteRawBits(negative, biasedExponent, significand));
  }

  public long toRawBits() {
    return bits;
  }

  public static Bid64 parseExact(String text) {
    String value = text.trim();
    boolean negative = value.startsWith("-");
    if (negative || value.startsWith("+")) {
      value = value.substring(1);
    }
    if (value.equalsIgnoreCase("Infinity") || value.equalsIgnoreCase("Inf")) {
      return negative ? NEGATIVE_INFINITY : POSITIVE_INFINITY;
    }
    if (value.equalsIgnoreCase("NaN")) {
      return fromRawBits((negative ? MASK_SIGN : 0) | MASK_NAN);
    }
    if (value.equalsIgnoreCase("SNaN")) {
      return fromRawBits((negative ? MASK_SIGN : 0) | MASK_SIGNALING_NAN);
    }

    int ePosition = Math.max(value.indexOf('E'), value.indexOf('e'));
    int explicitExponent = ePosition < 0
        ? 0
        : Integer.parseInt(value.substring(ePosition + 1));
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
    int exponent = Math.subtractExact(explicitExponent, fractionalDigits);
    while (digits.length() > 16 && digits.endsWith("0")) {
      digits = digits.substring(0, digits.length() - 1);
      exponent = Math.incrementExact(exponent);
    }
    if (digits.length() > 16) {
      throw new ArithmeticException("value is not exactly representable as BID64");
    }
    int biasedExponent = Math.addExact(exponent, 398);
    if (biasedExponent < 0 || biasedExponent > 767) {
      throw new ArithmeticException("value is outside the BID64 exponent range");
    }
    return finite(negative, biasedExponent, Long.parseLong(digits));
  }

  public static Bid64 parse(String text, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.fromString(text, mode, flags));
  }

  /**
   * Converts a finite Java decimal value using the requested IEEE rounding mode.
   *
   * <p>The input's decimal scale is preserved when it fits the decimal64
   * exponent and precision. Arithmetic remains independent of
   * {@link BigDecimal}; this method is an interoperability boundary.
   */
  public static Bid64 fromBigDecimal(
      BigDecimal value, RoundingMode mode, StatusFlags flags) {
    return parse(value.toString(), mode, flags);
  }

  /**
   * Converts a Java decimal value exactly, preserving its scale when representable.
   *
   * @throws ArithmeticException if the value is not exactly representable as decimal64
   */
  public static Bid64 fromBigDecimalExact(BigDecimal value) {
    return parseExact(value.toString());
  }

  public static Bid64 fromDouble(double value, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.fromBinary64(value, mode, flags));
  }

  public static Bid64 fromFloat(float value, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.fromBinary32(value, mode, flags));
  }

  public static Bid64 fromLong(long value, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.fromInt64(value, mode, flags));
  }

  public long toLong(RoundingMode mode, StatusFlags flags) {
    StatusFlags conversionFlags = new StatusFlags();
    long result = Bid64Raw.toInt64(bits, mode, conversionFlags, true);
    flags.raise(conversionFlags.bits());
    if (conversionFlags.contains(StatusFlags.INVALID)) {
      throw new ArithmeticException("BID64 value is outside the long range");
    }
    return result;
  }

  public double toDouble(RoundingMode mode, StatusFlags flags) {
    return Bid64Raw.toBinary64(bits, mode, flags);
  }

  public float toFloat(RoundingMode mode, StatusFlags flags) {
    return Bid64Raw.toBinary32(bits, mode, flags);
  }

  public Binary128 toBinary128(RoundingMode mode, StatusFlags flags) {
    long[] bits128 = new long[2];
    Bid64Raw.toBinary128(bits, mode, flags, bits128);
    return Binary128.fromRawBits(bits128[0], bits128[1]);
  }

  public static Bid64 fromBinary128(
      Binary128 value, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(
        Bid64Raw.fromBinary128(value.highBits(), value.lowBits(), mode, flags));
  }

  public Bid128 toBid128(StatusFlags flags) {
    long[] result = new long[2];
    Bid64Raw.toBid128(bits, result, flags);
    return Bid128.fromRawBits(result[0], result[1]);
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
    return sign + significand() + "E" + signedExponent(biasedExponent() - 398);
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
      throw new ArithmeticException("non-finite BID64 has no BigDecimal representation");
    }
    return new BigDecimal(toCanonicalString());
  }

  public boolean isSigned() {
    return (bits & MASK_SIGN) != 0;
  }

  public boolean isFinite() {
    return (bits & MASK_INFINITY) != MASK_INFINITY;
  }

  public boolean isInfinite() {
    return (bits & MASK_INFINITY) == MASK_INFINITY
        && (bits & MASK_NAN) != MASK_NAN;
  }

  public boolean isNaN() {
    return (bits & MASK_NAN) == MASK_NAN;
  }

  public boolean isSignalingNaN() {
    return (bits & MASK_SIGNALING_NAN) == MASK_SIGNALING_NAN;
  }

  public boolean isCanonical() {
    if (isNaN()) {
      return (bits & 0x01fc_0000_0000_0000L) == 0
          && (bits & 0x0003_ffff_ffff_ffffL) <= MAX_NAN_PAYLOAD;
    }
    if ((bits & MASK_INFINITY) == MASK_INFINITY) {
      return (bits & 0x03ff_ffff_ffff_ffffL) == 0;
    }
    return !usesLargeSignificandEncoding() || rawSignificand() <= MAX_SIGNIFICAND;
  }

  public boolean isZero() {
    if (!isFinite()) {
      return false;
    }
    return significand() == 0 || !isCanonical();
  }

  public boolean isNormal() {
    return classify().isNormal();
  }

  public boolean isSubnormal() {
    return classify().isSubnormal();
  }

  public Bid64 negate() {
    return fromRawBits(bits ^ MASK_SIGN);
  }

  public Bid64 abs() {
    return fromRawBits(bits & ~MASK_SIGN);
  }

  public Bid64 copySign(Bid64 signSource) {
    return fromRawBits((bits & ~MASK_SIGN) | (signSource.bits & MASK_SIGN));
  }

  /**
   * Returns whether this value and {@code other} have the same quantum.
   *
   * <p>All NaNs have the same quantum, as do all infinities. A NaN and an
   * infinity do not have the same quantum.
   */
  public boolean sameQuantum(Bid64 other) {
    if (isNaN() || other.isNaN()) {
      return isNaN() && other.isNaN();
    }
    if (isInfinite() || other.isInfinite()) {
      return isInfinite() && other.isInfinite();
    }
    return biasedExponent() == other.biasedExponent();
  }

  /**
   * IEEE 754 totalOrder: true iff this value precedes or equals {@code other}
   * in the total ordering of encodings.
   */
  public boolean totalOrder(Bid64 other) {
    return totalOrderBits(bits, other.bits);
  }

  /**
   * IEEE 754 totalOrderMag: totalOrder of the absolute values.
   */
  public boolean totalOrderMag(Bid64 other) {
    return totalOrderBits(bits & ~MASK_SIGN, other.bits & ~MASK_SIGN);
  }

  public Bid64 add(Bid64 other, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.add(bits, other.bits, mode, flags));
  }

  public Bid64 subtract(Bid64 other, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.sub(bits, other.bits, mode, flags));
  }

  public Bid64 multiply(Bid64 other, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.mul(bits, other.bits, mode, flags));
  }

  public Bid64 divide(Bid64 other, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.div(bits, other.bits, mode, flags));
  }

  public Bid64 sqrt(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.sqrt(bits, mode, flags));
  }

  public Bid64 cbrt(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.cbrt(bits, mode, flags));
  }

  public Bid64 exp(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.exp(bits, mode, flags));
  }

  public Bid64 expm1(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.expm1(bits, mode, flags));
  }

  public Bid64 exp2(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.exp2(bits, mode, flags));
  }

  public Bid64 exp10(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.exp10(bits, mode, flags));
  }

  public Bid64 log(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.log(bits, mode, flags));
  }

  public Bid64 log10(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.log10(bits, mode, flags));
  }

  public Bid64 log2(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.log2(bits, mode, flags));
  }

  public Bid64 log1p(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.log1p(bits, mode, flags));
  }

  public Bid64 pow(Bid64 y, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.pow(bits, y.bits, mode, flags));
  }

  public Bid64 hypot(Bid64 y, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.hypot(bits, y.bits, mode, flags));
  }

  public Bid64 sin(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.sin(bits, mode, flags));
  }

  public Bid64 cos(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.cos(bits, mode, flags));
  }

  public Bid64 tan(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.tan(bits, mode, flags));
  }

  public Bid64 asin(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.asin(bits, mode, flags));
  }

  public Bid64 acos(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.acos(bits, mode, flags));
  }

  public Bid64 atan(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.atan(bits, mode, flags));
  }

  /**
   * Two-argument arctangent. This value is y; {@code x} is the x argument.
   */
  public Bid64 atan2(Bid64 x, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.atan2(bits, x.bits, mode, flags));
  }

  public Bid64 sinh(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.sinh(bits, mode, flags));
  }

  public Bid64 cosh(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.cosh(bits, mode, flags));
  }

  public Bid64 tanh(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.tanh(bits, mode, flags));
  }

  public Bid64 asinh(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.asinh(bits, mode, flags));
  }

  public Bid64 acosh(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.acosh(bits, mode, flags));
  }

  public Bid64 atanh(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.atanh(bits, mode, flags));
  }

  public Bid64 erf(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.erf(bits, mode, flags));
  }

  public Bid64 erfc(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.erfc(bits, mode, flags));
  }

  public Bid64 tgamma(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.tgamma(bits, mode, flags));
  }

  public Bid64 lgamma(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.lgamma(bits, mode, flags));
  }

  public Bid64 fma(Bid64 y, Bid64 z, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.fma(bits, y.bits, z.bits, mode, flags));
  }

  public Bid64 quantize(Bid64 exponent, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.quantize(bits, exponent.bits, mode, flags));
  }

  public Bid64 remainder(Bid64 other, StatusFlags flags) {
    return fromRawBits(Bid64Raw.rem(bits, other.bits, flags));
  }

  public Bid64 nextUp(StatusFlags flags) {
    return fromRawBits(Bid64Raw.nextUp(bits, flags));
  }

  public Bid64 nextDown(StatusFlags flags) {
    return fromRawBits(Bid64Raw.nextDown(bits, flags));
  }

  public Bid64 roundIntegral(RoundingMode mode, boolean exact, StatusFlags flags) {
    return fromRawBits(Bid64Raw.roundIntegral(bits, mode, flags, exact));
  }

  public Bid64 nearbyInt(RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.nearbyint(bits, mode, flags));
  }

  public Bid64 scaleByPowerOfTen(int n, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.scalbn(bits, n, mode, flags));
  }

  public Bid64 fmod(Bid64 other, StatusFlags flags) {
    return fromRawBits(Bid64Raw.fmod(bits, other.bits, flags));
  }

  public Bid64 nextAfter(Bid64 target, StatusFlags flags) {
    return fromRawBits(Bid64Raw.nextAfter(bits, target.bits, flags));
  }

  public Bid64 minNum(Bid64 other, StatusFlags flags) {
    return fromRawBits(Bid64Raw.minnum(bits, other.bits, flags));
  }

  public Bid64 maxNum(Bid64 other, StatusFlags flags) {
    return fromRawBits(Bid64Raw.maxnum(bits, other.bits, flags));
  }

  public Bid64 minNumMagnitude(Bid64 other, StatusFlags flags) {
    return fromRawBits(Bid64Raw.minnumMag(bits, other.bits, flags));
  }

  public Bid64 maxNumMagnitude(Bid64 other, StatusFlags flags) {
    return fromRawBits(Bid64Raw.maxnumMag(bits, other.bits, flags));
  }

  public Bid64 positiveDifference(
      Bid64 other, RoundingMode mode, StatusFlags flags) {
    return fromRawBits(Bid64Raw.fdim(bits, other.bits, mode, flags));
  }

  public int quantumExponent(StatusFlags flags) {
    return Bid64Raw.quantexp(bits, flags);
  }

  public Bid64 quantum() {
    return fromRawBits(Bid64Raw.quantum(bits));
  }

  public int ilogb(StatusFlags flags) {
    return Bid64Raw.ilogb(bits, flags);
  }

  public Bid64 logb(StatusFlags flags) {
    return fromRawBits(Bid64Raw.logb(bits, flags));
  }

  public static Bid64 copy(Bid64 x) {
    return x;
  }

  public static Bid64 nan() {
    return QUIET_NAN;
  }

  public static Bid64 inf() {
    return POSITIVE_INFINITY;
  }

  public static int radix() {
    return Bid64Raw.RADIX;
  }

  public boolean quietEqual(Bid64 other, StatusFlags flags) {
    return compare(other, flags, false) == 0;
  }

  public boolean quietNotEqual(Bid64 other, StatusFlags flags) {
    return compare(other, flags, false) != 0;
  }

  public boolean quietLess(Bid64 other, StatusFlags flags) {
    return compare(other, flags, false) == -1;
  }

  public boolean quietLessEqual(Bid64 other, StatusFlags flags) {
    int comparison = compare(other, flags, false);
    return comparison == -1 || comparison == 0;
  }

  public boolean quietGreater(Bid64 other, StatusFlags flags) {
    return compare(other, flags, false) == 1;
  }

  public boolean quietGreaterEqual(Bid64 other, StatusFlags flags) {
    int comparison = compare(other, flags, false);
    return comparison == 0 || comparison == 1;
  }

  public boolean quietOrdered(Bid64 other, StatusFlags flags) {
    return compare(other, flags, false) != UNORDERED;
  }

  public boolean quietUnordered(Bid64 other, StatusFlags flags) {
    return compare(other, flags, false) == UNORDERED;
  }

  public boolean quietGreaterUnordered(Bid64 other, StatusFlags flags) {
    int comparison = compare(other, flags, false);
    return comparison == 1 || comparison == UNORDERED;
  }

  public boolean quietLessUnordered(Bid64 other, StatusFlags flags) {
    int comparison = compare(other, flags, false);
    return comparison == -1 || comparison == UNORDERED;
  }

  public boolean quietNotGreater(Bid64 other, StatusFlags flags) {
    return compare(other, flags, false) != 1;
  }

  public boolean quietNotLess(Bid64 other, StatusFlags flags) {
    return compare(other, flags, false) != -1;
  }

  public boolean signalingLess(Bid64 other, StatusFlags flags) {
    return compare(other, flags, true) == -1;
  }

  public boolean signalingEqual(Bid64 other, StatusFlags flags) {
    return compare(other, flags, true) == 0;
  }

  public boolean signalingNotEqual(Bid64 other, StatusFlags flags) {
    return compare(other, flags, true) != 0;
  }

  public boolean signalingLessEqual(Bid64 other, StatusFlags flags) {
    int comparison = compare(other, flags, true);
    return comparison == -1 || comparison == 0;
  }

  public boolean signalingGreater(Bid64 other, StatusFlags flags) {
    return compare(other, flags, true) == 1;
  }

  public boolean signalingGreaterEqual(Bid64 other, StatusFlags flags) {
    int comparison = compare(other, flags, true);
    return comparison == 0 || comparison == 1;
  }

  public boolean signalingOrdered(Bid64 other, StatusFlags flags) {
    return compare(other, flags, true) != UNORDERED;
  }

  public boolean signalingUnordered(Bid64 other, StatusFlags flags) {
    return compare(other, flags, true) == UNORDERED;
  }

  public boolean signalingGreaterUnordered(Bid64 other, StatusFlags flags) {
    int comparison = compare(other, flags, true);
    return comparison == 1 || comparison == UNORDERED;
  }

  public boolean signalingLessUnordered(Bid64 other, StatusFlags flags) {
    int comparison = compare(other, flags, true);
    return comparison == -1 || comparison == UNORDERED;
  }

  public boolean signalingNotGreater(Bid64 other, StatusFlags flags) {
    return compare(other, flags, true) != 1;
  }

  public boolean signalingNotLess(Bid64 other, StatusFlags flags) {
    return compare(other, flags, true) != -1;
  }

  private int compare(Bid64 other, StatusFlags flags, boolean signaling) {
    boolean unordered = isNaN() || other.isNaN();
    if (unordered) {
      if (signaling || isSignalingNaN() || other.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      return UNORDERED;
    }
    return compareNumericBits(bits, other.bits);
  }

  public DecimalClass classify() {
    if (isNaN()) {
      return isSignalingNaN() ? DecimalClass.SIGNALING_NAN : DecimalClass.QUIET_NAN;
    }
    if (isInfinite()) {
      return isSigned() ? DecimalClass.NEGATIVE_INFINITY : DecimalClass.POSITIVE_INFINITY;
    }

    long coefficient = significand();
    if (coefficient == 0 || !isCanonical()) {
      return isSigned() ? DecimalClass.NEGATIVE_ZERO : DecimalClass.POSITIVE_ZERO;
    }

    boolean subnormal = isSubnormal(coefficient, biasedExponent());
    if (subnormal) {
      return isSigned() ? DecimalClass.NEGATIVE_SUBNORMAL : DecimalClass.POSITIVE_SUBNORMAL;
    }
    return isSigned() ? DecimalClass.NEGATIVE_NORMAL : DecimalClass.POSITIVE_NORMAL;
  }

  int biasedExponent() {
    return biasedExponentBits(bits);
  }

  long significand() {
    long value = rawSignificand();
    return value <= MAX_SIGNIFICAND ? value : 0L;
  }

  private long rawSignificand() {
    return rawSignificandBits(bits);
  }

  private boolean usesLargeSignificandEncoding() {
    return (bits & MASK_STEERING_BITS) == MASK_STEERING_BITS;
  }

  private static boolean isSubnormal(long coefficient, int biasedExponent) {
    if (biasedExponent >= 15) {
      return false;
    }
    UInt128 adjusted = UInt128.multiply(coefficient, POW10[biasedExponent]);
    return adjusted.high() == 0
        && Long.compareUnsigned(adjusted.low(), MIN_NORMAL_SIGNIFICAND) < 0;
  }

  private static boolean totalOrderBits(long x, long y) {
    if ((x & MASK_NAN) == MASK_NAN) {
      return totalOrderLeftNaN(x, y);
    }
    if ((y & MASK_NAN) == MASK_NAN) {
      return (y & MASK_SIGN) == 0;
    }
    if (x == y) {
      return true;
    }
    boolean xNegative = (x & MASK_SIGN) != 0;
    boolean yNegative = (y & MASK_SIGN) != 0;
    if (xNegative != yNegative) {
      return xNegative;
    }
    if ((x & MASK_INFINITY) == MASK_INFINITY) {
      return xNegative || (y & MASK_INFINITY) == MASK_INFINITY;
    }
    if ((y & MASK_INFINITY) == MASK_INFINITY) {
      return !yNegative;
    }

    int expX = biasedExponentBits(x);
    long sigX = rawSignificandBits(x);
    boolean xZero = sigX == 0 || sigX > MAX_SIGNIFICAND;
    int expY = biasedExponentBits(y);
    long sigY = rawSignificandBits(y);
    boolean yZero = sigY == 0 || sigY > MAX_SIGNIFICAND;

    if (xZero && yZero) {
      if (expX == expY) {
        return true;
      }
      return (expX <= expY) != xNegative;
    }
    if (xZero) {
      return !yNegative;
    }
    if (yZero) {
      return xNegative;
    }
    if (sigX > sigY && expX >= expY) {
      return xNegative;
    }
    if (sigX < sigY && expX <= expY) {
      return !xNegative;
    }
    if (expX - expY > 15) {
      return xNegative;
    }
    if (expY - expX > 15) {
      return !xNegative;
    }
    if (expX > expY) {
      UInt128 scaled = UInt128.multiply(sigX, POW10[expX - expY]);
      if (scaled.high() == 0 && scaled.low() == sigY) {
        return (expX <= expY) != xNegative;
      }
      boolean less = scaled.high() == 0 && Long.compareUnsigned(scaled.low(), sigY) < 0;
      return less != xNegative;
    }
    UInt128 scaled = UInt128.multiply(sigY, POW10[expY - expX]);
    if (scaled.high() == 0 && scaled.low() == sigX) {
      return (expX <= expY) != xNegative;
    }
    boolean less = scaled.high() != 0 || Long.compareUnsigned(sigX, scaled.low()) < 0;
    return less != xNegative;
  }

  private static int compareNumericBits(long x, long y) {
    if (x == y) {
      return 0;
    }
    boolean xInfinity = (x & MASK_INFINITY) == MASK_INFINITY;
    boolean yInfinity = (y & MASK_INFINITY) == MASK_INFINITY;
    if (xInfinity) {
      if (yInfinity) {
        return (x & MASK_SIGN) == (y & MASK_SIGN)
            ? 0
            : ((x & MASK_SIGN) != 0 ? -1 : 1);
      }
      return (x & MASK_SIGN) != 0 ? -1 : 1;
    }
    if (yInfinity) {
      return (y & MASK_SIGN) != 0 ? 1 : -1;
    }

    long sigX = rawSignificandBits(x);
    long sigY = rawSignificandBits(y);
    if (sigX > MAX_SIGNIFICAND) {
      sigX = 0;
    }
    if (sigY > MAX_SIGNIFICAND) {
      sigY = 0;
    }
    if (sigX == 0 && sigY == 0) {
      return 0;
    }
    if (sigX == 0) {
      return (y & MASK_SIGN) != 0 ? 1 : -1;
    }
    if (sigY == 0) {
      return (x & MASK_SIGN) != 0 ? -1 : 1;
    }

    boolean xNegative = (x & MASK_SIGN) != 0;
    boolean yNegative = (y & MASK_SIGN) != 0;
    if (xNegative != yNegative) {
      return xNegative ? -1 : 1;
    }
    int magnitude = compareMagnitude(
        sigX, biasedExponentBits(x), sigY, biasedExponentBits(y));
    return xNegative ? -magnitude : magnitude;
  }

  private static int compareMagnitude(long sigX, int expX, long sigY, int expY) {
    if (sigX == sigY && expX == expY) {
      return 0;
    }
    if (sigX > sigY && expX >= expY) {
      return 1;
    }
    if (sigX < sigY && expX <= expY) {
      return -1;
    }
    if (expX - expY > 15) {
      return 1;
    }
    if (expY - expX > 15) {
      return -1;
    }
    if (expX > expY) {
      UInt128 scaledX = UInt128.multiply(sigX, POW10[expX - expY]);
      return scaledX.compareTo(UInt128.fromLong(sigY));
    }
    UInt128 scaledY = UInt128.multiply(sigY, POW10[expY - expX]);
    return -scaledY.compareTo(UInt128.fromLong(sigX));
  }

  private static boolean totalOrderLeftNaN(long x, long y) {
    boolean xNegative = (x & MASK_SIGN) != 0;
    boolean yNan = (y & MASK_NAN) == MASK_NAN;
    boolean yNegative = (y & MASK_SIGN) != 0;
    if (xNegative) {
      if (!yNan || !yNegative) {
        return true;
      }
      boolean xSnan = (x & MASK_SIGNALING_NAN) == MASK_SIGNALING_NAN;
      boolean ySnan = (y & MASK_SIGNALING_NAN) == MASK_SIGNALING_NAN;
      if (xSnan == ySnan) {
        return compareNegativeNaNPayloads(x, y);
      }
      return ySnan;
    }
    if (!yNan || yNegative) {
      return false;
    }
    boolean xSnan = (x & MASK_SIGNALING_NAN) == MASK_SIGNALING_NAN;
    boolean ySnan = (y & MASK_SIGNALING_NAN) == MASK_SIGNALING_NAN;
    if (xSnan == ySnan) {
      return comparePositiveNaNPayloads(x, y);
    }
    return xSnan;
  }

  private static boolean comparePositiveNaNPayloads(long x, long y) {
    long payloadX = nanPayload(x);
    long payloadY = nanPayload(y);
    if (payloadX == 0) {
      return true;
    }
    if (payloadY == 0) {
      return false;
    }
    return payloadX <= payloadY;
  }

  private static boolean compareNegativeNaNPayloads(long x, long y) {
    long payloadX = nanPayload(x);
    long payloadY = nanPayload(y);
    if (payloadY == 0) {
      return true;
    }
    if (payloadX == 0) {
      return false;
    }
    return payloadX >= payloadY;
  }

  private static long nanPayload(long bits) {
    long payload = bits & 0x0003_ffff_ffff_ffffL;
    return payload == 0 || payload > MAX_NAN_PAYLOAD ? 0L : payload;
  }

  static long finiteRawBits(boolean negative, int biasedExponent, long significand) {
    long sign = negative ? MASK_SIGN : 0L;
    if (significand <= MASK_BINARY_SIGNIFICAND1) {
      return sign | ((long) biasedExponent << 53) | significand;
    }
    return sign
        | MASK_STEERING_BITS
        | ((long) biasedExponent << 51)
        | (significand & MASK_BINARY_SIGNIFICAND2);
  }

  static int biasedExponentBits(long bits) {
    if ((bits & MASK_STEERING_BITS) == MASK_STEERING_BITS) {
      return (int) ((bits & MASK_BINARY_EXPONENT2) >>> 51);
    }
    return (int) ((bits & MASK_BINARY_EXPONENT1) >>> 53);
  }

  static long significandBits(long bits) {
    long value = rawSignificandBits(bits);
    return value <= MAX_SIGNIFICAND ? value : 0L;
  }

  static long rawSignificandBits(long bits) {
    if ((bits & MASK_STEERING_BITS) == MASK_STEERING_BITS) {
      return (bits & MASK_BINARY_SIGNIFICAND2) | MASK_BINARY_OR2;
    }
    return bits & MASK_BINARY_SIGNIFICAND1;
  }

  private static String signedExponent(int exponent) {
    return exponent >= 0 ? "+" + exponent : Integer.toString(exponent);
  }

  /**
   * Orders all encodings using IEEE 754 {@code totalOrder}.
   *
   * <p>Unlike the quiet comparison methods, this ordering includes NaNs,
   * signed zeros, and distinct cohorts and is consistent with bitwise
   * {@link #equals(Object)}.
   */
  @Override
  public int compareTo(Bid64 other) {
    if (bits == other.bits) {
      return 0;
    }
    boolean before = totalOrder(other);
    boolean after = other.totalOrder(this);
    if (before != after) {
      return before ? -1 : 1;
    }
    return Long.compareUnsigned(bits, other.bits);
  }

  @Override
  public boolean equals(Object other) {
    return this == other || other instanceof Bid64 && bits == ((Bid64) other).bits;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(bits);
  }

  @Override
  public String toString() {
    return String.format("Bid64[0x%016x,%s]", bits, classify());
  }
}
