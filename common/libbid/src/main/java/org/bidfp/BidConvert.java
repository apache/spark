/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

/** String, integer, binary, and width conversions for BID64 and BID128. */
final class BidConvert {
  private BidConvert() {
  }

  static long fromString64(String text, RoundingMode mode, StatusFlags flags) {
    ParsedDecimal parsed = parse(text, false);
    if (parsed.special != 0L) {
      return parsed.special;
    }
    DecNum number = parsed.number;
    return number.packBid64(mode, flags);
  }

  static void fromString128(
      String text, RoundingMode mode, StatusFlags flags, long[] payloadOut) {
    ParsedDecimal parsed = parse(text, true);
    if (parsed.special != 0L) {
      payloadOut[0] = parsed.special;
      payloadOut[1] = 0L;
      return;
    }
    parsed.number.packBid128(mode, flags, payloadOut);
  }

  static String toString64(long x) {
    if (Bid64Raw.isNaN(x)) {
      String sign = Bid64Raw.isSigned(x) ? "-" : "+";
      return sign + (Bid64Raw.isSignalingNaN(x) ? "SNaN" : "NaN");
    }
    if (Bid64Raw.isInf(x)) {
      return (Bid64Raw.isSigned(x) ? "-" : "+") + "Inf";
    }
    long coeff = Bid64.significandBits(x);
    int exp = Bid64.biasedExponentBits(x) - 398;
    String digits = Long.toUnsignedString(coeff);
    return (Bid64Raw.isSigned(x) ? "-" : "+") + digits + "E" + signed(exp);
  }

  static String toString128(long high, long low) {
    Bid128 value = Bid128.fromRawBits(high, low);
    if (value.isNaN()) {
      return (value.isSigned() ? "-" : "+") + (value.isSignalingNaN() ? "SNaN" : "NaN");
    }
    if (value.isInfinite()) {
      return (value.isSigned() ? "-" : "+") + "Inf";
    }
    int exp = value.biasedExponent() - 6176;
    String coefficient = value.isZero() ? "0" : value.coefficient().toDecimalString();
    return (value.isSigned() ? "-" : "+")
        + coefficient
        + "E"
        + signed(exp);
  }

  static long fromInt64To64(long value, RoundingMode mode, StatusFlags flags) {
    if (value >= -PowersOfTen.MAX_16 && value <= PowersOfTen.MAX_16) {
      boolean negative = value < 0;
      long magnitude = negative ? -value : value;
      return Bid64.finiteRawBits(negative, 398, magnitude);
    }
    DecNum number = DecNum.ofLong(value);
    return number.packBid64(mode, flags);
  }

  static void fromInt64To128(long value, RoundingMode mode, StatusFlags flags, long[] out) {
    boolean negative = value < 0;
    long magnitude = value == Long.MIN_VALUE ? value : Math.abs(value);
    if (value == Long.MIN_VALUE) {
      DecNum.ofLong(value).packBid128(mode, flags, out);
      return;
    }
    Bid128 result = Bid128.finite(negative, 6176, 0L, magnitude);
    DecNum.store128(result, out);
  }

  static long fromUInt64To64(long value, RoundingMode mode, StatusFlags flags) {
    return DecNum.ofUnsigned(0L, value).packBid64(mode, flags);
  }

  static void fromUInt64To128(
      long value, RoundingMode mode, StatusFlags flags, long[] out) {
    DecNum.ofUnsigned(0L, value).packBid128(mode, flags, out);
  }

  static long toInt64(
      long x,
      RoundingMode mode,
      StatusFlags flags,
      boolean signed,
      int width,
      boolean signalInexact) {
    int initialFlags = flags.bits();
    if (!Bid64Raw.isFinite(x) || Bid64Raw.isNaN(x)) {
      flags.raise(StatusFlags.INVALID);
      return integerIndefinite(width);
    }
    if (Bid64Raw.isZero(x)) {
      return 0L;
    }
    long integral = BidIntegral.round64(x, mode, flags, signalInexact);
    long coeff = Bid64.significandBits(integral);
    int exp = Bid64.biasedExponentBits(integral) - 398;
    DecNum number = DecNum.ofCoefficient(Bid64Raw.isSigned(integral), coeff, exp);
    number.roundToDigits(width == 64 ? 19 : 10, RoundingMode.TOWARD_ZERO, new StatusFlags());
    if (number.exp() > 0) {
      if (number.digitCount() + number.exp() > 20) {
        flags.raise(StatusFlags.INVALID);
        return integerIndefinite(width);
      }
      int exponent = number.exp();
      number.multiplyPow10(exponent);
      number.shiftExp(-exponent);
    }
    while (number.exp() < 0) {
      boolean[] sticky = {false};
      number.dividePow10(-number.exp(), sticky);
    }
    UInt128 magnitude = number.toUInt128();
    long result = toBounded(magnitude, number.isNegative(), signed, width, flags);
    suppressInexactOnInvalid(flags, initialFlags);
    return result;
  }

  static long toInt64From128(
      long high,
      long low,
      RoundingMode mode,
      StatusFlags flags,
      boolean signed,
      int width,
      boolean signalInexact) {
    int initialFlags = flags.bits();
    Bid128 value = Bid128.fromRawBits(high, low);
    if (!value.isFinite() || value.isNaN()) {
      flags.raise(StatusFlags.INVALID);
      return integerIndefinite(width);
    }
    if (value.isZero()) {
      return 0L;
    }
    long[] rounded = new long[2];
    BidIntegral.round128(high, low, mode, flags, signalInexact, rounded);
    Bid128 integral = Bid128.fromRawBits(rounded[0], rounded[1]);
    UInt128 coeff = integral.coefficient();
    int exp = integral.biasedExponent() - 6176;
    DecNum number = DecNum.ofUnsigned(coeff.high(), coeff.low());
    number = applySign(number, integral.isSigned());
    number = scaleExp(number, exp);
    if (number.exp() > 0) {
      if (number.digitCount() + number.exp() > 20) {
        flags.raise(StatusFlags.INVALID);
        return integerIndefinite(width);
      }
      int exponent = number.exp();
      number.multiplyPow10(exponent);
      number.shiftExp(-exponent);
    }
    while (number.exp() < 0) {
      boolean[] sticky = {false};
      number.dividePow10(-number.exp(), sticky);
    }
    long result = toBounded(number.toUInt128(), number.isNegative(), signed, width, flags);
    suppressInexactOnInvalid(flags, initialFlags);
    return result;
  }

  static long fromBinary64To64(double value, RoundingMode mode, StatusFlags flags) {
    long bits = Double.doubleToRawLongBits(value);
    if ((bits & 0x7ff0_0000_0000_0000L) == 0L
        && (bits & 0x000f_ffff_ffff_ffffL) != 0L) {
      flags.raise(StatusFlags.DENORMAL);
    }
    if (Double.isNaN(value)) {
      if (isSnan(bits)) {
        flags.raise(StatusFlags.INVALID);
      }
      long payload = (bits << 13) >>> 14;
      if (payload > 999_999_999_999_999L) {
        payload = 0L;
      }
      return (bits & Bid64.MASK_SIGN) | Bid64.MASK_NAN | payload;
    }
    if (value == Double.POSITIVE_INFINITY) {
      return Bid64.POSITIVE_INFINITY.toRawBits();
    }
    if (value == Double.NEGATIVE_INFINITY) {
      return Bid64.NEGATIVE_INFINITY.toRawBits();
    }
    if (value == 0.0) {
      return Bid64.finiteRawBits(Math.copySign(1.0, value) < 0.0, 398, 0L);
    }
    DecNum number = fromBinary(value);
    return number.packBid64(mode, flags);
  }

  static long fromBinary32To64(float value, RoundingMode mode, StatusFlags flags) {
    int bits = Float.floatToRawIntBits(value);
    int exponent = bits & 0x7f80_0000;
    int fraction = bits & 0x007f_ffff;
    if (exponent == 0x7f80_0000 && fraction != 0) {
      if ((fraction & 0x0040_0000) == 0) {
        flags.raise(StatusFlags.INVALID);
      }
      long payload = (long) (fraction & 0x003f_ffff) << 28;
      if (payload > 999_999_999_999_999L) {
        payload = 0L;
      }
      return ((long) bits << 32 & Bid64.MASK_SIGN) | Bid64.MASK_NAN | payload;
    }
    long result = fromBinary64To64(value, mode, flags);
    if (exponent == 0 && fraction != 0) {
      flags.raise(StatusFlags.DENORMAL);
    }
    return result;
  }

  static void fromBinary64To128(
      double value, RoundingMode mode, StatusFlags flags, long[] payloadOut) {
    long bits = Double.doubleToRawLongBits(value);
    if ((bits & 0x7ff0_0000_0000_0000L) == 0L
        && (bits & 0x000f_ffff_ffff_ffffL) != 0L) {
      flags.raise(StatusFlags.DENORMAL);
    }
    if (Double.isNaN(value)) {
      if (isSnan(bits)) {
        flags.raise(StatusFlags.INVALID);
      }
      long binaryPayload = bits << 13;
      UInt128 payload = new UInt128(binaryPayload >>> 18, binaryPayload << 46);
      if (payload.compareTo(PowersOfTen.MAX_33) > 0) {
        payload = UInt128.ZERO;
      }
      payloadOut[0] = (bits & Bid128.MASK_SIGN) | Bid128.MASK_NAN | payload.high();
      payloadOut[1] = payload.low();
      return;
    }
    if (value == Double.POSITIVE_INFINITY) {
      DecNum.store128(Bid128.POSITIVE_INFINITY, payloadOut);
      return;
    }
    if (value == Double.NEGATIVE_INFINITY) {
      DecNum.store128(Bid128.NEGATIVE_INFINITY, payloadOut);
      return;
    }
    if (value == 0.0) {
      DecNum.store128(
          Bid128.finite(Math.copySign(1.0, value) < 0.0, 6176, 0L, 0L),
          payloadOut);
      return;
    }
    fromBinary(value).packBid128(mode, flags, payloadOut);
  }

  static void fromBinary32To128(
      float value, RoundingMode mode, StatusFlags flags, long[] payloadOut) {
    int bits = Float.floatToRawIntBits(value);
    int exponent = bits & 0x7f80_0000;
    int fraction = bits & 0x007f_ffff;
    if (exponent == 0x7f80_0000 && fraction != 0) {
      if ((fraction & 0x0040_0000) == 0) {
        flags.raise(StatusFlags.INVALID);
      }
      UInt128 payload = new UInt128((long) (fraction & 0x003f_ffff) << 24, 0L);
      if (payload.compareTo(PowersOfTen.MAX_33) > 0) {
        payload = UInt128.ZERO;
      }
      payloadOut[0] = ((long) bits << 32 & Bid128.MASK_SIGN)
          | Bid128.MASK_NAN
          | payload.high();
      payloadOut[1] = payload.low();
      return;
    }
    fromBinary64To128(value, mode, flags, payloadOut);
    if (exponent == 0 && fraction != 0) {
      flags.raise(StatusFlags.DENORMAL);
    }
  }

  static double toBinary64From64(long x, RoundingMode mode, StatusFlags flags) {
    if (Bid64Raw.isNaN(x)) {
      if (Bid64Raw.isSignalingNaN(x)) {
        flags.raise(StatusFlags.INVALID);
      }
      long payload = x & 0x0003_ffff_ffff_ffffL;
      if (payload > 999_999_999_999_999L) {
        payload = 0L;
      }
      long bits = (x & Bid64.MASK_SIGN) | 0x7ff8_0000_0000_0000L | (payload << 1);
      return Double.longBitsToDouble(bits);
    }
    if (Bid64Raw.isInf(x)) {
      return Bid64Raw.isSigned(x) ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
    }
    if (Bid64Raw.isZero(x)) {
      return Bid64Raw.isSigned(x) ? -0.0 : 0.0;
    }
    return toBinary(Bid64.significandBits(x), 0L, Bid64.biasedExponentBits(x) - 398,
        Bid64Raw.isSigned(x), mode, flags);
  }

  static double toBinary64From128(
      long high, long low, RoundingMode mode, StatusFlags flags) {
    Bid128 value = Bid128.fromRawBits(high, low);
    if (value.isNaN()) {
      if (value.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      UInt128 payload = new UInt128(high & 0x0000_3fff_ffff_ffffL, low);
      if (payload.compareTo(PowersOfTen.MAX_33) > 0) {
        payload = UInt128.ZERO;
      }
      long converted = (payload.high() << 18) | (payload.low() >>> 46);
      long bits = (high & Bid128.MASK_SIGN)
          | 0x7ff8_0000_0000_0000L
          | (converted >>> 13);
      return Double.longBitsToDouble(bits);
    }
    if (value.isInfinite()) {
      return value.isSigned() ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
    }
    if (value.isZero()) {
      return value.isSigned() ? -0.0 : 0.0;
    }
    UInt128 coeff = value.coefficient();
    return toBinary(
        coeff.low(),
        coeff.high(),
        value.biasedExponent() - 6176,
        value.isSigned(),
        mode,
        flags);
  }

  static float toBinary32From64(long x, RoundingMode mode, StatusFlags flags) {
    if (Bid64Raw.isNaN(x)) {
      if (Bid64Raw.isSignalingNaN(x)) {
        flags.raise(StatusFlags.INVALID);
      }
      long payload = x & 0x0003_ffff_ffff_ffffL;
      if (payload > 999_999_999_999_999L) {
        payload = 0L;
      }
      int bits = (int) (x >>> 32) & 0x8000_0000
          | 0x7fc0_0000
          | (int) (payload >>> 28);
      return Float.intBitsToFloat(bits);
    }
    if (Bid64Raw.isInf(x)) {
      return Bid64Raw.isSigned(x) ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
    }
    if (Bid64Raw.isZero(x)) {
      return Bid64Raw.isSigned(x) ? -0.0f : 0.0f;
    }
    DecNum number = DecNum.ofCoefficient(
        Bid64Raw.isSigned(x),
        Bid64.significandBits(x),
        Bid64.biasedExponentBits(x) - 398);
    return toBinary32(number, mode, flags);
  }

  static float toBinary32From128(long high, long low, RoundingMode mode, StatusFlags flags) {
    Bid128 value = Bid128.fromRawBits(high, low);
    if (value.isNaN()) {
      if (value.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      UInt128 payload = new UInt128(high & 0x0000_3fff_ffff_ffffL, low);
      if (payload.compareTo(PowersOfTen.MAX_33) > 0) {
        payload = UInt128.ZERO;
      }
      long converted = (payload.high() << 18) | (payload.low() >>> 46);
      int bits = (int) (high >>> 32) & 0x8000_0000
          | 0x7fc0_0000
          | (int) (converted >>> 42);
      return Float.intBitsToFloat(bits);
    }
    if (value.isInfinite()) {
      return value.isSigned() ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
    }
    if (value.isZero()) {
      return value.isSigned() ? -0.0f : 0.0f;
    }
    UInt128 coefficient = value.coefficient();
    DecNum number = DecNum.ofUnsigned(coefficient.high(), coefficient.low());
    if (value.isSigned()) {
      number.setNegative();
    }
    number.shiftExp(value.biasedExponent() - 6176);
    return toBinary32(number, mode, flags);
  }

  static void toBinary128From64(
      long x, RoundingMode mode, StatusFlags flags, long[] out) {
    BidBinary128Convert.toBinary128From64(x, mode, flags, out);
  }

  static void toBinary128From128(
      long high, long low, RoundingMode mode, StatusFlags flags, long[] out) {
    BidBinary128Convert.toBinary128From128(high, low, mode, flags, out);
  }

  static long fromBinary128To64(
      long high, long low, RoundingMode mode, StatusFlags flags) {
    return BidBinary128Convert.fromBinary128To64(high, low, mode, flags);
  }

  static void fromBinary128To128(
      long high, long low, RoundingMode mode, StatusFlags flags, long[] out) {
    BidBinary128Convert.fromBinary128To128(high, low, mode, flags, out);
  }

  static void bid64ToBid128(long x, long[] payloadOut, StatusFlags flags) {
    if (Bid64Raw.isNaN(x)) {
      if (Bid64Raw.isSignalingNaN(x)) {
        flags.raise(StatusFlags.INVALID);
      }
      long payload = x & 0x0003_ffff_ffff_ffffL;
      if (payload > 999_999_999_999_999L) {
        payload = 0L;
      }
      UInt128 scaled = UInt128.fromLong(payload).multiply(PowersOfTen.LONG[18]);
      long high = (x & 0xfc00_0000_0000_0000L) | scaled.high();
      if (Bid64Raw.isSignalingNaN(x)) {
        high = (high & ~0x0200_0000_0000_0000L) | Bid128.MASK_NAN;
      }
      payloadOut[0] = high;
      payloadOut[1] = scaled.low();
      return;
    }
    if (Bid64Raw.isInf(x)) {
      payloadOut[0] = (x & Bid64.MASK_SIGN) | Bid128.MASK_INFINITY;
      payloadOut[1] = 0L;
      return;
    }
    long coeff = Bid64.significandBits(x);
    int exp = Bid64.biasedExponentBits(x) - 398;
    int biased = exp + 6176;
    Bid128 result = Bid128.finite(Bid64Raw.isSigned(x), biased, 0L, coeff);
    DecNum.store128(result, payloadOut);
  }

  static long bid128ToBid64(long high, long low, RoundingMode mode, StatusFlags flags) {
    Bid128 value = Bid128.fromRawBits(high, low);
    if (value.isNaN()) {
      if (value.isSignalingNaN()) {
        flags.raise(StatusFlags.INVALID);
      }
      UInt128 payload = new UInt128(high & 0x0000_3fff_ffff_ffffL, low);
      if (payload.compareTo(PowersOfTen.MAX_33) > 0) {
        payload = UInt128.ZERO;
      }
      long converted = payload.divide(PowersOfTen.LONG[18]).quotient().low();
      return (high & 0xfc00_0000_0000_0000L) | converted;
    }
    if (value.isInfinite()) {
      return (value.isSigned() ? Bid64.MASK_SIGN : 0L) | Bid64.MASK_INFINITY;
    }
    UInt128 coeff = value.coefficient();
    int exp = value.biasedExponent() - 6176;
    DecNum number = DecNum.ofUnsigned(coeff.high(), coeff.low());
    if (value.isSigned()) {
      number = applySign(number, true);
    }
    number = scaleExp(number, exp);
    return number.packBid64(mode, flags);
  }

  static DecNum fromBinaryPublic(double value) {
    return fromBinary(value);
  }

  private static DecNum fromBinary(double value) {
    long bits = Double.doubleToRawLongBits(value);
    boolean negative = bits < 0L;
    int binaryExp = (int) ((bits >>> 52) & 0x7ffL);
    long fraction = bits & 0x000f_ffff_ffff_ffffL;
    long mantissa;
    int exp2;
    if (binaryExp == 0) {
      mantissa = fraction;
      exp2 = -1074;
    } else {
      mantissa = fraction | (1L << 52);
      exp2 = binaryExp - 1075;
    }
    return BidBinary128Convert.fromBinary(new UInt128(0L, mantissa), exp2, negative);
  }

  private static double toBinary(
      long coeffLow,
      long coeffHigh,
      int exp10,
      boolean negative,
      RoundingMode mode,
      StatusFlags flags) {
    DecNum number = DecNum.ofUnsigned(coeffHigh, coeffLow);
    if (negative) {
      number = applySign(number, true);
    }
    number = scaleExp(number, exp10);
    String digits = number.toDigits();
    int scale = number.exp();
    double nearest = Double.parseDouble(digits + "e" + scale);
    if (Double.isInfinite(nearest)) {
      boolean toInfinity = mode == RoundingMode.TIES_TO_EVEN
          || mode == RoundingMode.TIES_AWAY
          || BidRound.overflowToInfinity(negative, mode);
      flags.raise(StatusFlags.INEXACT);
      DecNum overflowThreshold = DecNum.ofLong(1L);
      overflowThreshold.multiplyPow2(1024);
      if (toInfinity || number.compareAbsolute(overflowThreshold) >= 0) {
        flags.raise(StatusFlags.OVERFLOW);
      }
      if (toInfinity) {
        return negative ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
      }
      return negative ? -Double.MAX_VALUE : Double.MAX_VALUE;
    }
    DecNum binary = fromBinary(Math.abs(nearest));
    int comparison = binary.compareAbsolute(number);
    if (comparison != 0) {
      flags.raise(StatusFlags.INEXACT);
      nearest = directedDouble(nearest, comparison, negative, mode);
      if (Double.isInfinite(nearest)) {
        flags.raise(StatusFlags.OVERFLOW);
      }
      if (isTinyBinary64(number, nearest, negative, mode)) {
        flags.raise(StatusFlags.UNDERFLOW);
      }
    }
    return negative ? -Math.abs(nearest) : Math.abs(nearest);
  }

  private static boolean isTinyBinary64(
      DecNum number, double nearest, boolean negative, RoundingMode mode) {
    long bits = Double.doubleToRawLongBits(nearest);
    if ((bits & 0x7ff0_0000_0000_0000L) == 0L) {
      return true;
    }
    if (nearest != Double.MIN_NORMAL
        || number.compareAbsolute(fromBinary(Double.MIN_NORMAL)) >= 0) {
      return false;
    }
    DecNum scaled = new DecNum();
    scaled.copyFrom(number);
    scaled.multiplySmall(4);
    DecNum threshold = fromBinary(Double.MIN_NORMAL);
    threshold.multiplySmall(4);
    threshold.subtractAbsolute(fromBinary(Double.MIN_VALUE));
    if (mode == RoundingMode.TIES_TO_EVEN || mode == RoundingMode.TIES_AWAY) {
      return scaled.compareAbsolute(threshold) < 0;
    }
    boolean roundsAway = mode == RoundingMode.TOWARD_POSITIVE && !negative
        || mode == RoundingMode.TOWARD_NEGATIVE && negative;
    if (!roundsAway) {
      return false;
    }
    scaled.copyFrom(number);
    scaled.multiplySmall(2);
    threshold = fromBinary(Double.MIN_NORMAL);
    threshold.multiplySmall(2);
    threshold.subtractAbsolute(fromBinary(Double.MIN_VALUE));
    return scaled.compareAbsolute(threshold) < 0;
  }

  private static double directedDouble(
      double nearest, int comparison, boolean negative, RoundingMode mode) {
    boolean towardSmallerMagnitude = mode == RoundingMode.TOWARD_ZERO
        || mode == RoundingMode.TOWARD_NEGATIVE && !negative
        || mode == RoundingMode.TOWARD_POSITIVE && negative;
    boolean towardLargerMagnitude = mode == RoundingMode.TOWARD_NEGATIVE && negative
        || mode == RoundingMode.TOWARD_POSITIVE && !negative;
    if (towardSmallerMagnitude && comparison > 0) {
      return Math.nextDown(nearest);
    }
    if (towardLargerMagnitude && comparison < 0) {
      return Math.nextUp(nearest);
    }
    return nearest;
  }

  private static float toBinary32(
      DecNum number, RoundingMode mode, StatusFlags flags) {
    boolean negative = number.isNegative();
    float nearest = Float.parseFloat(number.toDigits() + "e" + number.exp());
    if (Float.isInfinite(nearest)) {
      boolean toInfinity = mode == RoundingMode.TIES_TO_EVEN
          || mode == RoundingMode.TIES_AWAY
          || BidRound.overflowToInfinity(negative, mode);
      flags.raise(StatusFlags.INEXACT);
      DecNum overflowThreshold = DecNum.ofLong(1L);
      overflowThreshold.multiplyPow2(128);
      if (toInfinity || number.compareAbsolute(overflowThreshold) >= 0) {
        flags.raise(StatusFlags.OVERFLOW);
      }
      if (toInfinity) {
        return negative ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
      }
      return negative ? -Float.MAX_VALUE : Float.MAX_VALUE;
    }
    DecNum binary = fromBinary(Math.abs((double) nearest));
    int comparison = binary.compareAbsolute(number);
    if (comparison != 0) {
      flags.raise(StatusFlags.INEXACT);
      nearest = directedFloat(nearest, comparison, negative, mode);
      if (Float.isInfinite(nearest)) {
        flags.raise(StatusFlags.OVERFLOW);
      }
      if (isTinyBinary32(number, nearest, negative, mode)) {
        flags.raise(StatusFlags.UNDERFLOW);
      }
    }
    return negative ? -Math.abs(nearest) : Math.abs(nearest);
  }

  private static float directedFloat(
      float nearest, int comparison, boolean negative, RoundingMode mode) {
    boolean towardSmallerMagnitude = mode == RoundingMode.TOWARD_ZERO
        || mode == RoundingMode.TOWARD_NEGATIVE && !negative
        || mode == RoundingMode.TOWARD_POSITIVE && negative;
    boolean towardLargerMagnitude = mode == RoundingMode.TOWARD_NEGATIVE && negative
        || mode == RoundingMode.TOWARD_POSITIVE && !negative;
    if (towardSmallerMagnitude && comparison > 0) {
      return Math.nextDown(nearest);
    }
    if (towardLargerMagnitude && comparison < 0) {
      return Math.nextUp(nearest);
    }
    return nearest;
  }

  private static boolean isTinyBinary32(
      DecNum number, float nearest, boolean negative, RoundingMode mode) {
    int bits = Float.floatToRawIntBits(nearest);
    if ((bits & 0x7f80_0000) == 0) {
      return true;
    }
    if (nearest != Float.MIN_NORMAL
        || number.compareAbsolute(fromBinary(Float.MIN_NORMAL)) >= 0) {
      return false;
    }
    DecNum scaled = new DecNum();
    scaled.copyFrom(number);
    scaled.multiplySmall(4);
    DecNum threshold = fromBinary(Float.MIN_NORMAL);
    threshold.multiplySmall(4);
    threshold.subtractAbsolute(fromBinary(Float.MIN_VALUE));
    if (mode == RoundingMode.TIES_TO_EVEN || mode == RoundingMode.TIES_AWAY) {
      return scaled.compareAbsolute(threshold) < 0;
    }
    boolean roundsAway = mode == RoundingMode.TOWARD_POSITIVE && !negative
        || mode == RoundingMode.TOWARD_NEGATIVE && negative;
    if (!roundsAway) {
      return false;
    }
    scaled.copyFrom(number);
    scaled.multiplySmall(2);
    threshold = fromBinary(Float.MIN_NORMAL);
    threshold.multiplySmall(2);
    threshold.subtractAbsolute(fromBinary(Float.MIN_VALUE));
    return scaled.compareAbsolute(threshold) < 0;
  }

  private static long toBounded(
      UInt128 magnitude, boolean negative, boolean signed, int width, StatusFlags flags) {
    if (magnitude.isZero()) {
      return 0L;
    }
    if (signed) {
      long negativeLimit = 1L << (width - 1);
      long positiveLimit = negativeLimit - 1;
      boolean outOfRange = magnitude.high() != 0L
          || negative && Long.compareUnsigned(magnitude.low(), negativeLimit) > 0
          || !negative && Long.compareUnsigned(magnitude.low(), positiveLimit) > 0;
      if (outOfRange) {
        flags.raise(StatusFlags.INVALID);
        return integerIndefinite(width);
      }
      return negative ? -magnitude.low() : magnitude.low();
    }
    long maximum = width == 64 ? -1L : (1L << width) - 1;
    if (negative || magnitude.high() != 0L
        || Long.compareUnsigned(magnitude.low(), maximum) > 0) {
      flags.raise(StatusFlags.INVALID);
      return integerIndefinite(width);
    }
    return magnitude.low();
  }

  private static long integerIndefinite(int width) {
    return 1L << (width - 1);
  }

  private static void suppressInexactOnInvalid(StatusFlags flags, int initialFlags) {
    if ((flags.bits() & StatusFlags.INVALID) != 0
        && (initialFlags & StatusFlags.INVALID) == 0) {
      flags.clear(StatusFlags.INEXACT);
      if ((initialFlags & StatusFlags.INEXACT) != 0) {
        flags.raise(StatusFlags.INEXACT);
      }
    }
  }

  private static DecNum applySign(DecNum number, boolean negative) {
    if (negative) {
      number.setNegative();
    }
    return number;
  }

  private static DecNum scaleExp(DecNum number, int exp) {
    number.shiftExp(exp);
    return number;
  }

  private static boolean isSnan(long doubleBits) {
    return (doubleBits & 0x7ff8_0000_0000_0000L) == 0x7ff0_0000_0000_0000L
        && (doubleBits & 0x0007_ffff_ffff_ffffL) != 0L
        && (doubleBits & 0x0008_0000_0000_0000L) == 0L;
  }

  private static String signed(int exponent) {
    return exponent >= 0 ? "+" + exponent : Integer.toString(exponent);
  }

  private static ParsedDecimal parse(String text, boolean bid128) {
    String value = text.trim();
    boolean negative = value.startsWith("-");
    if (negative || value.startsWith("+")) {
      value = value.substring(1);
    }
    ParsedDecimal parsed = new ParsedDecimal();
    if (value.equalsIgnoreCase("Infinity") || value.equalsIgnoreCase("Inf")) {
      parsed.special = (negative ? Bid64.MASK_SIGN : 0L) | Bid64.MASK_INFINITY;
      return parsed;
    }
    if (value.regionMatches(true, 0, "SNaN", 0, 4)) {
      parsed.special = (negative ? Bid64.MASK_SIGN : 0L) | Bid64.MASK_SIGNALING_NAN;
      parsed.signaling = true;
      return parsed;
    }
    if (value.regionMatches(true, 0, "NaN", 0, 3)
        || value.regionMatches(true, 0, "QNaN", 0, 4)) {
      parsed.special = (negative ? Bid64.MASK_SIGN : 0L) | Bid64.MASK_NAN;
      return parsed;
    }
    int ePosition = Math.max(value.indexOf('E'), value.indexOf('e'));
    int explicit = 0;
    if (ePosition >= 0) {
      String exponent = value.substring(ePosition + 1);
      if (bid128 && exponent.endsWith("E")) {
        exponent = exponent.substring(0, exponent.length() - 1);
      }
      try {
        explicit = Integer.parseInt(exponent);
      } catch (NumberFormatException exception) {
        parsed.special = Bid64.MASK_NAN;
        return parsed;
      }
      value = value.substring(0, ePosition);
    }
    int point = value.indexOf('.');
    int fraction = point < 0 ? 0 : value.length() - point - 1;
    String digits = point < 0 ? value : value.substring(0, point) + value.substring(point + 1);
    if (digits.isEmpty()) {
      digits = "0";
    }
    DecNum number = new DecNum();
    number.clear();
    for (int i = 0; i < digits.length(); i++) {
      char c = digits.charAt(i);
      if (c < '0' || c > '9') {
        parsed.special = Bid64.MASK_NAN;
        parsed.signaling = true;
        return parsed;
      }
      number.multiplyBy10();
      number.addDigit(c - '0');
    }
    if (negative) {
      number.setNegative();
    }
    number.shiftExp(explicit - fraction);
    parsed.number = number;
    return parsed;
  }

  private static final class ParsedDecimal {
    DecNum number;
    long special;
    boolean signaling;
  }
}
