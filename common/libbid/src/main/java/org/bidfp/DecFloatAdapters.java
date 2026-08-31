/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bidfp;

/**
 * Spark SQL adapters for BID values.
 *
 * <p>These methods implement SQL equality and ordering, not IEEE
 * {@code totalOrder}. Signed zeros compare equal, all NaNs compare equal, and
 * NaN sorts after finite values.
 */
public final class DecFloatAdapters {
  private static final int BID64_MAX_QUANTUM = 369;
  private static final int BID128_MAX_QUANTUM = 6111;

  private DecFloatAdapters() {
  }

  /** Returns the Spark SQL ordering of two decimal64 payloads. */
  public static int sqlCompare64(long a, long b) {
    if (Bid64Raw.isNaN(a)) {
      return Bid64Raw.isNaN(b) ? 0 : 1;
    }
    if (Bid64Raw.isNaN(b)) {
      return -1;
    }
    StatusFlags flags = new StatusFlags();
    if (Bid64Raw.quietLess(a, b, flags)) {
      return -1;
    }
    if (Bid64Raw.quietGreater(a, b, flags)) {
      return 1;
    }
    return 0;
  }

  /** Returns whether two decimal64 payloads are equal under Spark SQL semantics. */
  public static boolean sqlEquals64(long a, long b) {
    return sqlCompare64(a, b) == 0;
  }

  /** Returns the Spark SQL ordering of two decimal128 payloads. */
  public static int sqlCompare128(long aHi, long aLo, long bHi, long bLo) {
    if (Bid128Raw.isNaN(aHi, aLo)) {
      return Bid128Raw.isNaN(bHi, bLo) ? 0 : 1;
    }
    if (Bid128Raw.isNaN(bHi, bLo)) {
      return -1;
    }
    StatusFlags flags = new StatusFlags();
    if (Bid128Raw.quietLess(aHi, aLo, bHi, bLo, flags)) {
      return -1;
    }
    if (Bid128Raw.quietGreater(aHi, aLo, bHi, bLo, flags)) {
      return 1;
    }
    return 0;
  }

  /** Returns whether two decimal128 payloads are equal under Spark SQL semantics. */
  public static boolean sqlEquals128(long aHi, long aLo, long bHi, long bLo) {
    return sqlCompare128(aHi, aLo, bHi, bLo) == 0;
  }

  /** Returns a hash consistent with {@link #sqlEquals64(long, long)}. */
  public static int sqlHash64(long payload) {
    return Long.hashCode(canonicalize64(payload));
  }

  /** Returns a hash consistent with SQL decimal128 equality. */
  public static int sqlHash128(long hi, long lo) {
    long[] canonical = new long[2];
    canonicalize128(hi, lo, canonical);
    int result = Long.hashCode(canonical[0]);
    return 31 * result + Long.hashCode(canonical[1]);
  }

  public static long sign64(long payload) {
    if (Bid64Raw.isNaN(payload) || Bid64Raw.isZero(payload)) {
      return payload;
    }
    return Bid64Raw.isSigned(payload)
        ? Bid64.finiteRawBits(true, 398, 1L)
        : Bid64.finiteRawBits(false, 398, 1L);
  }

  public static void sign128(long hi, long lo, long[] out) {
    if (Bid128Raw.isNaN(hi, lo) || Bid128Raw.isZero(hi, lo)) {
      out[0] = hi;
      out[1] = lo;
      return;
    }
    boolean negative = Bid128Raw.isSigned(hi, lo);
    DecNum.store128(Bid128.finite(negative, 6176, 0L, 1L), out);
  }

  public static long canonicalize64(long payload) {
    if (Bid64Raw.isNaN(payload)) {
      return Bid64.MASK_NAN;
    }
    if (Bid64Raw.isInf(payload)) {
      return (payload & Bid64.MASK_SIGN) | Bid64.MASK_INFINITY;
    }
    long coeff = Bid64.significandBits(payload);
    int biased = Bid64.biasedExponentBits(payload);
    if (coeff == 0L) {
      return Bid64.finiteRawBits(false, 398, 0L);
    }
    while (coeff % 10L == 0L && biased < 767) {
      coeff /= 10L;
      biased++;
    }
    return Bid64.finiteRawBits(Bid64Raw.isSigned(payload), biased, coeff);
  }

  public static void canonicalize128(long hi, long lo, long[] out) {
    Bid128 value = Bid128.fromRawBits(hi, lo);
    if (value.isNaN()) {
      DecNum.store128(Bid128.QUIET_NAN, out);
      return;
    }
    if (value.isInfinite()) {
      out[0] = (hi & Bid128.MASK_SIGN) | Bid128.MASK_INFINITY;
      out[1] = 0L;
      return;
    }
    UInt128 coeff = value.coefficient();
    int biased = value.biasedExponent();
    if (coeff.isZero()) {
      DecNum.store128(Bid128.finite(false, 6176, 0L, 0L), out);
      return;
    }
    while (!coeff.isZero()) {
      UInt128.Division division = coeff.divide(10L);
      if (division.remainder() != 0L || biased >= 12_287) {
        break;
      }
      coeff = division.quotient();
      biased++;
    }
    DecNum.store128(
        Bid128.finite(value.isSigned(), biased, coeff.high(), coeff.low()),
        out);
  }

  public static long roundToScale64(
      long payload, long targetExponent, int rounding, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    RoundingMode mode = RoundingMode.fromIntel(rounding);
    if (Bid64Raw.isNaN(payload) || Bid64Raw.isInf(payload) || Bid64Raw.isZero(payload)) {
      flags.copyTo(statusOut);
      return payload;
    }
    if (Bid64Raw.isFinite(payload) && !Bid64Raw.isNaN(payload)) {
      int quantum = BidScale.quantexp64(payload);
      if (targetExponent <= quantum) {
        flags.copyTo(statusOut);
        return payload;
      }
    }
    if (targetExponent > BID64_MAX_QUANTUM) {
      long result = roundCoarse64(payload, targetExponent, mode, flags);
      flags.copyTo(statusOut);
      return result;
    }
    long exemplar = Bid64.finiteRawBits(false, (int) targetExponent + 398, 1L);
    long result = Bid64Raw.quantize(payload, exemplar, mode, flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static void roundToScale128(
      long hi,
      long lo,
      long targetExponent,
      int rounding,
      long[] out,
      int[] statusOut) {
    RoundingMode mode = RoundingMode.fromIntel(rounding);
    StatusFlags flags = new StatusFlags();
    if (Bid128Raw.isNaN(hi, lo)
        || Bid128Raw.isInf(hi, lo)
        || Bid128Raw.isZero(hi, lo)) {
      out[0] = hi;
      out[1] = lo;
      flags.copyTo(statusOut);
      return;
    }
    if (Bid128Raw.isFinite(hi, lo) && !Bid128Raw.isNaN(hi, lo)
        && targetExponent <= BidScale.quantexp128(hi, lo)) {
      out[0] = hi;
      out[1] = lo;
      flags.copyTo(statusOut);
      return;
    }
    if (targetExponent > BID128_MAX_QUANTUM) {
      roundCoarse128(hi, lo, targetExponent, mode, flags, out);
      flags.copyTo(statusOut);
      return;
    }
    Bid128 exemplar = Bid128.finite(false, (int) targetExponent + 6176, 0L, 1L);
    Bid128Raw.quantize(
        hi, lo, exemplar.highBits(), exemplar.lowBits(), mode, flags, out);
    flags.copyTo(statusOut);
  }

  public static long fromDecimal64(
      long unscaledHi, long unscaledLo, int scale, int rounding, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    DecNum number = DecNum.ofUnsigned(unscaledHi, unscaledLo);
    if (unscaledHi < 0L) {
      DecNum mag = DecNum.ofUnsigned(~unscaledHi, -unscaledLo);
      if (unscaledLo == 0L) {
        mag = DecNum.ofUnsigned(~unscaledHi + 1, 0L);
      }
      number = mag;
      number.setNegative();
    }
    number.shiftExp(-scale);
    long result = number.packBid64(RoundingMode.fromIntel(rounding), flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static int toDecimal64(
      long payload,
      int targetPrecision,
      int targetScale,
      int rounding,
      long[] unscaledOut,
      int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    if (!validDecimalType(targetPrecision, targetScale)
        || !Bid64Raw.isFinite(payload)
        || Bid64Raw.isNaN(payload)) {
      return invalidDecimal(unscaledOut, statusOut, flags);
    }
    int[] roundingStatus = {0};
    long rounded = roundToScale64(payload, -(long) targetScale, rounding, roundingStatus);
    flags.raise(roundingStatus[0]);
    if (!Bid64Raw.isFinite(rounded) || Bid64Raw.isNaN(rounded)) {
      return invalidDecimal(unscaledOut, statusOut, flags);
    }
    DecNum decimal = DecNum.ofUnsigned(0L, Bid64.significandBits(rounded));
    int exponent = Bid64.biasedExponentBits(rounded) - 398;
    if (!decimal.isZero()) {
      int scaleDelta = exponent + targetScale;
      if (scaleDelta < 0 || decimal.digitCount() + scaleDelta > targetPrecision) {
        return invalidDecimal(unscaledOut, statusOut, flags);
      }
      decimal.multiplyPow10(scaleDelta);
    }
    if (decimal.digitCount() > targetPrecision) {
      return invalidDecimal(unscaledOut, statusOut, flags);
    }
    UInt128 coeff = decimal.toUInt128();
    if (Bid64Raw.isSigned(rounded) && !coeff.isZero()) {
      storeNegative(coeff, unscaledOut);
    } else {
      unscaledOut[0] = coeff.high();
      unscaledOut[1] = coeff.low();
    }
    flags.copyTo(statusOut);
    return targetScale;
  }

  public static void fromDecimal128(
      long unscaledHi, long unscaledLo, int scale, int rounding, long[] payloadOut,
      int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    DecNum number = DecNum.ofUnsigned(unscaledHi, unscaledLo);
    if (unscaledHi < 0L) {
      long magnitudeLow = -unscaledLo;
      long magnitudeHigh = ~unscaledHi + (magnitudeLow == 0L ? 1L : 0L);
      number = DecNum.ofUnsigned(magnitudeHigh, magnitudeLow);
      number.setNegative();
    }
    number.shiftExp(-scale);
    number.packBid128(RoundingMode.fromIntel(rounding), flags, payloadOut);
    flags.copyTo(statusOut);
  }

  public static int toDecimal128(
      long hi,
      long lo,
      int targetPrecision,
      int targetScale,
      int rounding,
      long[] unscaledOut,
      int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    Bid128 value = Bid128.fromRawBits(hi, lo);
    if (!validDecimalType(targetPrecision, targetScale)
        || !value.isFinite()
        || value.isNaN()) {
      return invalidDecimal(unscaledOut, statusOut, flags);
    }
    long[] rounded = new long[2];
    int[] roundingStatus = {0};
    roundToScale128(
        hi, lo, -(long) targetScale, rounding, rounded, roundingStatus);
    flags.raise(roundingStatus[0]);
    value = Bid128.fromRawBits(rounded[0], rounded[1]);
    if (!value.isFinite() || value.isNaN()) {
      return invalidDecimal(unscaledOut, statusOut, flags);
    }
    DecNum decimal = DecNum.ofUnsigned(
        value.coefficient().high(), value.coefficient().low());
    int exponent = value.biasedExponent() - 6176;
    if (!decimal.isZero()) {
      int scaleDelta = exponent + targetScale;
      if (scaleDelta < 0 || decimal.digitCount() + scaleDelta > targetPrecision) {
        return invalidDecimal(unscaledOut, statusOut, flags);
      }
      decimal.multiplyPow10(scaleDelta);
    }
    if (decimal.digitCount() > targetPrecision) {
      return invalidDecimal(unscaledOut, statusOut, flags);
    }
    UInt128 coeff = decimal.toUInt128();
    if (value.isSigned() && !coeff.isZero()) {
      storeNegative(coeff, unscaledOut);
    } else {
      unscaledOut[0] = coeff.high();
      unscaledOut[1] = coeff.low();
    }
    flags.copyTo(statusOut);
    return targetScale;
  }

  private static boolean validDecimalType(int precision, int scale) {
    return precision >= 1 && precision <= 38 && scale <= 38 && scale <= precision;
  }

  private static int invalidDecimal(
      long[] unscaledOut, int[] statusOut, StatusFlags flags) {
    flags.raise(StatusFlags.INVALID);
    unscaledOut[0] = 0L;
    unscaledOut[1] = 0L;
    flags.copyTo(statusOut);
    return 0;
  }

  private static void storeNegative(UInt128 magnitude, long[] unscaledOut) {
    long signedLow = -magnitude.low();
    unscaledOut[0] = ~magnitude.high() + (signedLow == 0L ? 1L : 0L);
    unscaledOut[1] = signedLow;
  }

  private static long roundCoarse64(
      long payload, long targetExponent, RoundingMode mode, StatusFlags flags) {
    long coeff = Bid64.significandBits(payload);
    int exp = Bid64.biasedExponentBits(payload) - 398;
    DecNum number = roundAtExponent(
        Bid64Raw.isSigned(payload),
        Long.toUnsignedString(coeff),
        exp,
        targetExponent,
        mode,
        flags,
        386);
    return number.packBid64(mode, flags);
  }

  private static void roundCoarse128(
      long hi, long lo, long targetExponent, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128 value = Bid128.fromRawBits(hi, lo);
    UInt128 coeff = value.coefficient();
    DecNum number = roundAtExponent(
        value.isSigned(),
        coeff.toDecimalString(),
        value.biasedExponent() - 6176,
        targetExponent,
        mode,
        flags,
        6146);
    number.packBid128(mode, flags, out);
  }

  private static DecNum roundAtExponent(
      boolean negative,
      String digits,
      int exponent,
      long targetExponent,
      RoundingMode mode,
      StatusFlags flags,
      int maximumBoundedExponent) {
    long discarded = exponent < 0 && targetExponent > Long.MAX_VALUE + exponent
        ? Long.MAX_VALUE
        : targetExponent - exponent;
    int kept = discarded >= digits.length() ? 0 : digits.length() - (int) discarded;
    int first = 0;
    boolean sticky = false;
    if (discarded == digits.length()) {
      first = digits.charAt(0) - '0';
      sticky = hasNonZero(digits, 1);
    } else if (discarded > digits.length()) {
      sticky = hasNonZero(digits, 0);
    } else {
      first = digits.charAt(kept) - '0';
      sticky = hasNonZero(digits, kept + 1);
    }
    DecNum rounded = new DecNum();
    rounded.clear();
    for (int i = 0; i < kept; i++) {
      rounded.multiplyBy10();
      rounded.addDigit(digits.charAt(i) - '0');
    }
    rounded.setNegative(negative);
    if (first != 0 || sticky) {
      flags.raise(StatusFlags.INEXACT);
    }
    if (BidRound.shouldIncrement(negative, rounded.low64(), first, sticky, mode)) {
      rounded.addOne();
    }
    rounded.shiftExp((int) Math.min(targetExponent, maximumBoundedExponent));
    return rounded;
  }

  private static boolean hasNonZero(String digits, int start) {
    for (int i = start; i < digits.length(); i++) {
      if (digits.charAt(i) != '0') {
        return true;
      }
    }
    return false;
  }
}
