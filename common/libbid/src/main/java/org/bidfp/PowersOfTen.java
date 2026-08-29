/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

/** Shared powers of ten for BID64 (16 digits) and BID128 (34 digits). */
final class PowersOfTen {
  static final long[] LONG = {
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
    1_000_000_000_000_000L,
    10_000_000_000_000_000L,
    100_000_000_000_000_000L,
    1_000_000_000_000_000_000L
  };

  static final UInt128 TEN_34 = parse("10000000000000000000000000000000000");
  static final UInt128 MAX_34 = parse("9999999999999999999999999999999999");
  static final UInt128 MAX_33 = parse("999999999999999999999999999999999");
  static final UInt128 TEN_16 = UInt128.fromLong(10_000_000_000_000_000L);
  static final long MAX_16 = 9_999_999_999_999_999L;

  private PowersOfTen() {
  }

  static int decimalDigits(long value) {
    if (value == 0L) {
      return 1;
    }
    int bits = 64 - Long.numberOfLeadingZeros(value);
    int digits = (((bits - 1) * 1233) >>> 12) + 1;
    if (digits < LONG.length && Long.compareUnsigned(value, LONG[digits]) >= 0) {
      digits++;
    }
    return digits;
  }

  static int decimalDigits(UInt128 value) {
    if (value.isZero()) {
      return 1;
    }
    int bitLength = value.high() == 0
        ? 64 - Long.numberOfLeadingZeros(value.low())
        : 128 - Long.numberOfLeadingZeros(value.high());
    int digits = bitLength * 1233 >>> 12;
    UInt128 threshold = pow10(digits);
    if (value.compareTo(threshold) >= 0) {
      digits++;
    }
    return digits;
  }

  static UInt128 pow10(int exponent) {
    if (exponent < 0) {
      throw new IllegalArgumentException("negative power of ten");
    }
    if (exponent < LONG.length) {
      return UInt128.fromLong(LONG[exponent]);
    }
    UInt128 result = UInt128.fromLong(1L);
    int remaining = exponent;
    while (remaining >= 18) {
      result = result.multiply(LONG[18]);
      remaining -= 18;
    }
    if (remaining != 0) {
      result = result.multiply(LONG[remaining]);
    }
    return result;
  }

  private static UInt128 parse(String digits) {
    UInt128 value = UInt128.ZERO;
    for (int i = 0; i < digits.length(); i++) {
      value = value.multiply(10L).add(digits.charAt(i) - '0');
    }
    return value;
  }
}
