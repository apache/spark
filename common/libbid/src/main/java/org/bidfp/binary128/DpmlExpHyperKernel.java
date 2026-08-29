/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

import org.bidfp.binary128.tables.ExpX;
import org.bidfp.binary128.tables.PowX;

/** Shared Intel QUAD UX machinery for the exponential and hyperbolic families. */
final class DpmlExpHyperKernel {
  private static final int EXP_DEGREE_OFFSET = 112;
  private static final int EXP_COEFS_OFFSET = 120;
  private static final int EXP10_DEGREE_OFFSET = 568;
  private static final int EXP10_COEFS_OFFSET = 576;
  private static final int EXP_LIMIT = 18;
  private static final int FORCED_OVERFLOW_EXPONENT = 131071;
  private static final int FORCED_UNDERFLOW_EXPONENT = -131072;

  private DpmlExpHyperKernel() {
  }

  static Reduction reduce(Unpacked argument, boolean base10, StatusFlags status) {
    long[] table = ExpX.TABLE;
    int constants = base10 ? ExpX.EXP10_CONSTANT_TABLE_ADDRESS
        : ExpX.EXP_CONSTANT_TABLE_ADDRESS;
    int exponent = argument.exponent;
    int reduceExponent = (int) UxTable.word64(table, constants + 16);

    if (exponent < reduceExponent - 1 || exponent > reduceExponent + 17) {
      Unpacked reduced = argument.copy();
      long scale = 0;
      if (exponent > 0) {
        reduced.exponent = -128;
        scale = argument.sign != 0
            ? FORCED_UNDERFLOW_EXPONENT : FORCED_OVERFLOW_EXPONENT;
      }
      return new Reduction(scale, reduced);
    }

    long reciprocal = UxTable.word64(table, constants);
    long ln2High = UxTable.word64(table, constants + 8);
    long scaleBits = multiplyHighUnsigned(argument.fracHi >>> 1, reciprocal);
    int shift = 61 - exponent;
    long increment = 1L << (shift - 1);
    scaleBits = (scaleBits + increment) & -(1L << shift);

    while (scaleBits > 0) {
      scaleBits <<= 1;
      shift++;
    }

    int scaleExponent = 64 - shift;
    long[] product = multiply128(scaleBits, ln2High);
    int productExponent = scaleExponent;
    if (product[0] >= 0) {
      productExponent--;
      product[0] = (product[0] << 1) | (product[1] >>> 63);
      product[1] <<= 1;
    }

    Unpacked highProduct = new Unpacked();
    highProduct.setNorm(
        argument.sign, productExponent + reduceExponent, product[0], product[1]);
    Unpacked highRemainder = sub(argument, highProduct, status);

    Unpacked uxScale = new Unpacked();
    uxScale.setNorm(argument.sign, scaleExponent, scaleBits, 0);
    Unpacked lowLn2 = UxTable.readUxFloat(table, constants + 24);
    Unpacked lowProduct = mul(uxScale, lowLn2, status);
    Unpacked reduced = sub(highRemainder, lowProduct, status);

    long scale = scaleBits >>> shift;
    return new Reduction(argument.sign != 0 ? -scale : scale, reduced);
  }

  /** Unpacked {@code exp}; used by tgamma so lgamma is packed only once. */
  static Unpacked exp(Unpacked x, StatusFlags status) {
    Reduction reduction = reduce(x, false, status);
    Unpacked result = expPolynomial(reduction.argument, false, status);
    result.exponent += (int) reduction.scale;
    return result;
  }

  static Unpacked expPolynomial(Unpacked reduced, boolean base10, StatusFlags status) {
    int degreeOffset = base10 ? EXP10_DEGREE_OFFSET : EXP_DEGREE_OFFSET;
    int coefsOffset = base10 ? EXP10_COEFS_OFFSET : EXP_COEFS_OFFSET;
    int degree = (int) UxTable.word64(ExpX.TABLE, degreeOffset);
    int flags = UxEval.STANDARD;
    Unpacked magnitude = reduced.copy();
    if (magnitude.sign != 0) {
      magnitude.sign = 0;
      flags |= UxEval.ALTERNATE_SIGN;
    }
    Unpacked result = new Unpacked();
    UxEval.evaluateRational(
        magnitude, ExpX.TABLE, coefsOffset, degree,
        UxEval.numeratorFlags(flags), result, status);
    return result;
  }

  static Unpacked expm1Polynomial(Unpacked reduced, StatusFlags status) {
    int degree = (int) UxTable.word64(ExpX.TABLE, EXP_DEGREE_OFFSET) - 1;
    int sign = reduced.sign;
    int flags = UxEval.POST_MULTIPLY;
    Unpacked magnitude = reduced.copy();
    if (sign != 0) {
      magnitude.sign = 0;
      flags |= UxEval.ALTERNATE_SIGN;
    }
    Unpacked result = new Unpacked();
    UxEval.evaluateRational(
        magnitude, ExpX.TABLE, EXP_COEFS_OFFSET, degree,
        UxEval.numeratorFlags(flags), result, status);
    result.exponent++;
    result.sign = sign;
    return result;
  }

  static Unpacked exp2(Unpacked x, StatusFlags status) {
    int exponent = x.exponent;
    if (exponent > EXP_LIMIT) {
      Unpacked forced = x.copy();
      forced.exponent = x.sign != 0
          ? FORCED_UNDERFLOW_EXPONENT : FORCED_OVERFLOW_EXPONENT;
      return forced;
    }
    long scale = 0;
    Unpacked reduced = x.copy();
    if (exponent >= 0) {
      scale = exponent == 0
          ? 1L
          : (x.fracHi >>> (64 - exponent))
              + ((x.fracHi >>> (63 - exponent)) & 1L);
      if (x.sign != 0) {
        scale = -scale;
      }
      reduced = sub(x, fromLong(scale), status);
    }

    int flags = UxEval.STANDARD;
    if (reduced.sign != 0) {
      reduced.sign = 0;
      flags |= UxEval.ALTERNATE_SIGN;
    }
    Unpacked result = new Unpacked();
    UxEval.evaluateRational(
        reduced, PowX.TABLE, PowX.POW2_COEF_ARRAY, PowX.POW2_COEF_ARRAY_DEGREE,
        UxEval.numeratorFlags(flags), result, status);
    result.exponent += (int) scale;
    return result;
  }

  static Unpacked[] sinhCoshPolynomial(
      Unpacked reduced, int flags, StatusFlags status) {
    Unpacked[] result = {new Unpacked(), new Unpacked()};
    UxEval.evaluateRational(
        reduced.copy(), ExpX.TABLE, ExpX.SINHCOSH_COEF_ARRAY,
        ExpX.SINHCOSH_COEF_ARRAY_DEGREE, flags, result, status);
    return result;
  }

  static Unpacked add(Unpacked a, Unpacked b, StatusFlags status) {
    Unpacked result = new Unpacked();
    UxOps.addsubUnpacked(a.copy(), b.copy(), result, status);
    return result;
  }

  static Unpacked sub(Unpacked a, Unpacked b, StatusFlags status) {
    Unpacked negative = b.copy();
    UxOps.negate(negative);
    return add(a, negative, status);
  }

  static Unpacked mul(Unpacked a, Unpacked b, StatusFlags status) {
    Unpacked result = new Unpacked();
    UxOps.mulUnpacked(a.copy(), b.copy(), result, status);
    return result;
  }

  static Unpacked div(Unpacked a, Unpacked b, StatusFlags status) {
    Unpacked result = new Unpacked();
    UxOps.divUnpacked(a.copy(), b.copy(), result, status);
    return result;
  }

  static Unpacked fromLong(long value) {
    if (value == 0) {
      Unpacked zero = new Unpacked();
      zero.setZero(0);
      return zero;
    }
    long magnitude = value < 0 ? -value : value;
    int leading = Long.numberOfLeadingZeros(magnitude);
    Unpacked result = new Unpacked();
    result.setNorm(
        value < 0 ? Unpacked.UX_SIGN_BIT : 0, 64 - leading, magnitude << leading, 0);
    return result;
  }

  static Binary128 quietNaN(Binary128 x, StatusFlags status) {
    if (x.isSignalingNaN()) {
      status.raise(StatusFlags.INVALID);
    }
    return Binary128.fromRawBits(
        x.highBits() | Binary128.QUIET_NAN_BIT, x.lowBits());
  }

  private static long multiplyHighUnsigned(long x, long y) {
    return Wide.umulh(x, y);
  }

  private static long[] multiply128(long x, long y) {
    return new long[] {Wide.umulh(x, y), x * y};
  }

  static final class Reduction {
    final long scale;
    final Unpacked argument;

    Reduction(long scale, Unpacked argument) {
      this.scale = scale;
      this.argument = argument;
    }
  }
}
