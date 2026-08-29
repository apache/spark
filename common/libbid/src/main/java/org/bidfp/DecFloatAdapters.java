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
 * DBR SQL adapters that are not Intel RDFP operations. Marked so this library
 * is not mistaken for a 1:1 Intel dump.
 */
public final class DecFloatAdapters {
  private static final int BID64_MAX_QUANTUM = 369;
  private static final int BID128_MAX_QUANTUM = 6111;

  private DecFloatAdapters() {
  }

  /** DBR total order: NaN greatest, all NaNs equal, signed zeros equal. */
  public static int compare64(long a, long b) {
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

  public static boolean equals64(long a, long b) {
    return compare64(a, b) == 0;
  }

  public static int compare128(long aHi, long aLo, long bHi, long bLo) {
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

  public static boolean equals128(long aHi, long aLo, long bHi, long bLo) {
    return compare128(aHi, aLo, bHi, bLo) == 0;
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

  public static long roundToScale64(long payload, long targetExponent, int rounding) {
    StatusFlags flags = new StatusFlags();
    RoundingMode mode = RoundingMode.fromIntel(rounding);
    if (Bid64Raw.isInf(payload)) {
      return payload;
    }
    if (Bid64Raw.isFinite(payload) && !Bid64Raw.isNaN(payload)) {
      int quantum = BidScale.quantexp64(payload);
      if (targetExponent <= quantum) {
        return payload;
      }
    }
    if (targetExponent > BID64_MAX_QUANTUM) {
      return roundCoarse64(payload, targetExponent, mode, flags);
    }
    long exemplar = Bid64.finiteRawBits(false, (int) targetExponent + 398, 1L);
    return Bid64Raw.quantize(payload, exemplar, mode, flags);
  }

  public static void roundToScale128(
      long hi, long lo, long targetExponent, int rounding, long[] out) {
    RoundingMode mode = RoundingMode.fromIntel(rounding);
    StatusFlags flags = new StatusFlags();
    if (Bid128Raw.isInf(hi, lo)) {
      out[0] = hi;
      out[1] = lo;
      return;
    }
    if (Bid128Raw.isFinite(hi, lo) && !Bid128Raw.isNaN(hi, lo)
        && targetExponent <= BidScale.quantexp128(hi, lo)) {
      out[0] = hi;
      out[1] = lo;
      return;
    }
    if (targetExponent > BID128_MAX_QUANTUM) {
      roundCoarse128(hi, lo, targetExponent, mode, flags, out);
      return;
    }
    Bid128 exemplar = Bid128.finite(false, (int) targetExponent + 6176, 0L, 1L);
    Bid128Raw.quantize(
        hi, lo, exemplar.highBits(), exemplar.lowBits(), mode, flags, out);
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

  public static int toDecimal64(long payload, long[] unscaledOut, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    if (!Bid64Raw.isFinite(payload) || Bid64Raw.isNaN(payload)) {
      flags.raise(StatusFlags.INVALID);
      flags.copyTo(statusOut);
      unscaledOut[0] = 0L;
      unscaledOut[1] = 0L;
      return 0;
    }
    long coeff = Bid64.significandBits(payload);
    int exp = Bid64.biasedExponentBits(payload) - 398;
    int scale = -exp;
    if (Bid64Raw.isSigned(payload) && coeff != 0L) {
      unscaledOut[0] = -1L;
      unscaledOut[1] = -coeff;
    } else {
      unscaledOut[0] = 0L;
      unscaledOut[1] = coeff;
    }
    flags.copyTo(statusOut);
    return scale;
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

  public static int toDecimal128(long hi, long lo, long[] unscaledOut, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    Bid128 value = Bid128.fromRawBits(hi, lo);
    if (!value.isFinite() || value.isNaN()) {
      flags.raise(StatusFlags.INVALID);
      flags.copyTo(statusOut);
      unscaledOut[0] = 0L;
      unscaledOut[1] = 0L;
      return 0;
    }
    UInt128 coeff = value.coefficient();
    if (value.isSigned()) {
      long signedLow = -coeff.low();
      unscaledOut[0] = ~coeff.high() + (signedLow == 0L ? 1L : 0L);
      unscaledOut[1] = signedLow;
    } else {
      unscaledOut[0] = coeff.high();
      unscaledOut[1] = coeff.low();
    }
    if (value.isSigned() && coeff.low() == 0L) {
      unscaledOut[0] = -coeff.high();
      unscaledOut[1] = 0L;
    }
    flags.copyTo(statusOut);
    return -(value.biasedExponent() - 6176);
  }

  private static long roundCoarse64(
      long payload, long targetExponent, RoundingMode mode, StatusFlags flags) {
    long coeff = Bid64.significandBits(payload);
    int exp = Bid64.biasedExponentBits(payload) - 398;
    DecNum number = DecNum.ofCoefficient(Bid64Raw.isSigned(payload), coeff, exp);
    number.shiftExp(-(int) Math.min(targetExponent, Integer.MAX_VALUE));
    number.roundToDigits(1, mode, flags);
    return number.packBid64(mode, flags);
  }

  private static void roundCoarse128(
      long hi, long lo, long targetExponent, RoundingMode mode, StatusFlags flags, long[] out) {
    Bid128 value = Bid128.fromRawBits(hi, lo);
    UInt128 coeff = value.coefficient();
    DecNum number = DecNum.ofUnsigned(coeff.high(), coeff.low());
    if (value.isSigned()) {
      number.setNegative();
    }
    number.shiftExp(value.biasedExponent() - 6176);
    number.shiftExp(-(int) Math.min(targetExponent, Integer.MAX_VALUE));
    number.roundToDigits(1, mode, flags);
    number.packBid128(mode, flags, out);
  }
}
