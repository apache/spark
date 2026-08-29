/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

import org.bidfp.binary128.tables.PowX;

/**
 * ANSI C power ported from Intel DPML {@code dpml_ux_pow.c}.
 */
public final class DpmlPow {
  private DpmlPow() {
  }

  public static Binary128 pow(
      Binary128 x, Binary128 y, RoundingMode mode, StatusFlags st) {
    // ANSI C mapping: x^+/-0 is one, including a NaN x.
    if (y.isZero()) {
      return Binary128.ONE;
    }
    if (x.isNaN()) {
      return DpmlPowCbrtSupport.quietNaN(x, st);
    }
    if (y.isNaN()) {
      return DpmlPowCbrtSupport.quietNaN(y, st);
    }

    int integerKind = DpmlPowCbrtSupport.integerKind(y);
    boolean odd = integerKind == 2;
    boolean negativeResult = x.isSigned() && odd;

    if (y.isInfinite()) {
      int cmp = DpmlPowCbrtSupport.compareAbsToOne(x);
      if (cmp == 0) {
        st.raise(StatusFlags.INVALID);
        return Binary128.canonicalNaN(true);
      }
      boolean returnInfinity = (cmp > 0) != y.isSigned();
      if (returnInfinity && x.isSigned()) {
        st.raise(StatusFlags.INVALID);
        return Binary128.canonicalNaN(true);
      }
      return returnInfinity ? Binary128.POSITIVE_INFINITY : Binary128.ZERO;
    }

    if (x.isZero()) {
      if (y.isSigned()) {
        st.raise(StatusFlags.DIVIDE_BY_ZERO);
        // Intel's POWER_ZERO_TO_NEG error result carries the sign bit.
        return Binary128.NEGATIVE_INFINITY;
      }
      return negativeResult ? Binary128.NEGATIVE_ZERO : Binary128.ZERO;
    }

    if (x.isInfinite()) {
      if (x.isSigned() && !y.isSigned()) {
        st.raise(StatusFlags.INVALID);
        return Binary128.canonicalNaN(true);
      }
      if (y.isSigned()) {
        if (x.isSigned()) {
          return Binary128.ZERO;
        }
        return negativeResult ? Binary128.NEGATIVE_ZERO : Binary128.ZERO;
      }
      return negativeResult ? Binary128.NEGATIVE_INFINITY : Binary128.POSITIVE_INFINITY;
    }

    if (x.isSigned() && integerKind == 0) {
      st.raise(StatusFlags.INVALID);
      return Binary128.canonicalNaN(true);
    }
    if (x.abs().equals(Binary128.ONE)) {
      return negativeResult ? Binary128.ONE.negate() : Binary128.ONE;
    }
    if (isHalf(y)) {
      Binary128 root = UxOps.sqrt(x, mode, st);
      return y.isSigned() ? UxOps.div(Binary128.ONE, root, mode, st) : root;
    }

    StatusFlags local = new StatusFlags();
    Unpacked ux = UxOps.unpack(x.abs());
    Unpacked uy = UxOps.unpack(y);
    Unpacked log2 = log2(ux, local);
    Unpacked product = new Unpacked();
    UxOps.mulUnpacked(uy, log2, product, local);
    Unpacked result = exp2(product, local);
    result.sign = negativeResult ? Unpacked.UX_SIGN_BIT : 0;
    return UxOps.pack(result, mode, st);
  }

  private static boolean isHalf(Binary128 value) {
    return (value.highBits() & ~Binary128.MASK_SIGN) == 0x3ffe_0000_0000_0000L
        && value.lowBits() == 0L;
  }

  private static Unpacked log2(Unpacked x, StatusFlags status) {
    UxOps.normalize(x);
    int exponent = x.exponent;
    long threshold = PowX.TABLE[PowX.ONE_OVER_SQRT_2 >>> 3];
    if (Long.compareUnsigned(x.fracHi, threshold) <= 0) {
      exponent--;
    }
    x.exponent -= exponent;

    Unpacked one = UxTable.readUxFloat(PowX.TABLE, PowX.UX_ONE);
    Unpacked sum = new Unpacked();
    Unpacked difference = new Unpacked();
    UxOps.addsubUnpacked(x, one, sum, status);
    one.sign = Unpacked.UX_SIGN_BIT;
    UxOps.addsubUnpacked(x, one, difference, status);

    Unpacked ratio = new Unpacked();
    Unpacked twoOverLn2 = UxTable.readUxFloat(PowX.TABLE, PowX.UX_TWO_OVER_LN2);
    UxOps.divUnpacked(twoOverLn2, sum, ratio, status);
    Unpacked z = new Unpacked();
    UxOps.mulUnpacked(ratio, difference, z, status);

    Unpacked z2 = new Unpacked();
    UxOps.mulUnpacked(z, z, z2, status);
    Unpacked tail = new Unpacked();
    evaluatePolynomial(
        z2, PowX.POW_LOG2_COEF_ARRAY, PowX.POW_LOG2_COEF_ARRAY_DEGREE, tail, status);
    Unpacked postMultiplied = new Unpacked();
    UxOps.mulUnpacked(z2, tail, postMultiplied, status);
    tail.copyFrom(postMultiplied);
    Unpacked z3p = new Unpacked();
    UxOps.mulUnpacked(z, tail, z3p, status);
    Unpacked low = new Unpacked();
    UxOps.addsubUnpacked(z, z3p, low, status);
    if (exponent == 0) {
      return low;
    }
    Unpacked result = new Unpacked();
    UxOps.addsubUnpacked(KernelEval.fromInt(exponent), low, result, status);
    return result;
  }

  private static Unpacked exp2(Unpacked x, StatusFlags status) {
    if (x.exponent > 18) {
      Unpacked result = x.copy();
      result.sign = 0;
      result.exponent = x.sign != 0 ? -131072 : 131071;
      return result;
    }
    int integer = DpmlPowCbrtSupport.nearestInt(x);
    Unpacked reduced;
    if (integer == 0) {
      reduced = x.copy();
    } else {
      Unpacked negativeInteger = KernelEval.fromInt(-integer);
      reduced = new Unpacked();
      UxOps.addsubUnpacked(x, negativeInteger, reduced, status);
    }
    Unpacked result = new Unpacked();
    evaluatePolynomial(
        reduced, PowX.POW2_COEF_ARRAY, PowX.POW2_COEF_ARRAY_DEGREE, result, status);
    result.exponent += integer;
    return result;
  }

  /**
   * The generated FIXED_128 banks are stored high degree first. This local
   * form mirrors Intel's Horner expansion without changing the shared evaluator.
   */
  private static void evaluatePolynomial(
      Unpacked argument, int offset, int degree, Unpacked result, StatusFlags status) {
    Unpacked accumulator = new Unpacked();
    Unpacked coefficient = new Unpacked();
    Unpacked product = new Unpacked();
    UxTable.fixed128ToUnpacked(PowX.TABLE, offset, accumulator);
    for (int i = 1; i <= degree; i++) {
      UxOps.mulUnpacked(accumulator, argument, product, status);
      UxTable.fixed128ToUnpacked(
          PowX.TABLE, offset + i * UxTable.FIXED_128_BYTES, coefficient);
      UxOps.addsubUnpacked(product, coefficient, accumulator, status);
    }
    accumulator.exponent += UxTable.readCoefScale(PowX.TABLE, offset, degree);
    result.copyFrom(accumulator);
  }
}
