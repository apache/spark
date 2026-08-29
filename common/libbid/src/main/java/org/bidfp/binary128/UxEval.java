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
 * Intel {@code EVALUATE_RATIONAL} / {@code EVALUATE_PACKED_POLY} for UX
 * tables ({@code dpml_ux_ops_64.c} / {@code dpml_ux_ops.c}), using unpacked
 * mul/add/div instead of the fixed-point Horner micro-kernel.
 *
 * <p>Flag layout matches {@code dpml_ux.h} for a 64-bit {@code WORD}.
 */
final class UxEval {
  /** Per-side poly form bits (also shifted for the denominator field). */
  static final int STANDARD = 0x001;
  static final int POST_MULTIPLY = 0x002;
  static final int SQUARE_TERM = 0x004;
  static final int ALTERNATE_SIGN = 0x008;

  static final int NUM_DEN_FIELD_WIDTH = 4;
  static final int NUMERATOR_MASK = (1 << NUM_DEN_FIELD_WIDTH) - 1;
  static final int DENOMINATOR_MASK = NUMERATOR_MASK << NUM_DEN_FIELD_WIDTH;

  static final int NO_DIVIDE = 1 << (2 * NUM_DEN_FIELD_WIDTH);
  static final int SWAP = 2 << (2 * NUM_DEN_FIELD_WIDTH);
  static final int SKIP = 4 << (2 * NUM_DEN_FIELD_WIDTH);

  /** Packed into the high bits of the {@code EVALUATE_RATIONAL} flags word. */
  private static final int SCALE_WIDTH = 6;
  private static final int SCALE_POS = 64 - SCALE_WIDTH;

  /** Intel {@code ADD} / {@code SUB} for packed-poly {@code ADDSUB}. */
  static final int ADD = 0;
  static final int SUB = 1;

  private UxEval() {
  }

  static int numeratorFlags(int n) {
    return n & NUMERATOR_MASK;
  }

  static int denominatorFlags(int n) {
    return (n & NUMERATOR_MASK) << NUM_DEN_FIELD_WIDTH;
  }

  static int either(int n) {
    return numeratorFlags(n) | denominatorFlags(n);
  }

  /** Pack a global argument scale into rational flags ({@code P_SCALE}). */
  static long packScale(int n) {
    return ((long) n) << SCALE_POS;
  }

  /** Unpack global argument scale from rational flags ({@code G_SCALE}). */
  static int getScale(long flags) {
    return (int) (flags >> SCALE_POS);
  }

  /**
   * {@code EVALUATE_RATIONAL}. Mutates {@code argument.exponent} by
   * {@code G_SCALE(flags)} like Intel.
   *
   * <p>{@code results} must provide {@code results[0]}. When both numerator
   * and denominator are evaluated, {@code results[1]} is used (allocated
   * temporarily if null). Final quotient is always
   * {@code results[0] / results[1]} when dividing (Intel {@code SWAP}).
   */
  static void evaluateRational(
      Unpacked argument,
      long[] table,
      int coefsOffset,
      int degree,
      long flags,
      Unpacked[] results,
      StatusFlags status) {
    if (results == null || results[0] == null) {
      throw new IllegalArgumentException("results[0]");
    }

    UxScratch.Frame scratch = UxScratch.acquire();
    try {
      Unpacked r0 = results[0];
      Unpacked r1 = results.length > 1 && results[1] != null
          ? results[1]
          : scratch.unpacked(0);
      evaluateRationalCore(
          argument, table, coefsOffset, degree, flags, r0, r1, status, scratch);
    } finally {
      UxScratch.release(scratch);
    }
  }

  private static void evaluateRationalCore(
      Unpacked argument,
      long[] table,
      int coefsOffset,
      int degree,
      long flags,
      Unpacked r0,
      Unpacked r1,
      StatusFlags status,
      UxScratch.Frame scratch) {
    long f = flags;
    long signFlags = f;
    argument.exponent += getScale(f);

    Unpacked polyArg;
    if ((f & either(SQUARE_TERM)) != 0) {
      polyArg = scratch.unpacked(1);
      UxOps.mulUnpacked(argument, argument, polyArg, status);
    } else {
      polyArg = argument;
      if (argument.sign != 0) {
        signFlags ^= either(ALTERNATE_SIGN);
      }
    }
    UxOps.normalize(polyArg);

    int bankBytes = UxTable.coefBankBytes(degree);
    int coefPtr = coefsOffset;

    // tmp = (!SWAP || SKIP) ? 0 : 1
    int slot = ((f & SWAP) == 0 || (f & SKIP) != 0) ? 0 : 1;
    Unpacked first = slot == 0 ? r0 : r1;
    Unpacked second = slot == 0 ? r1 : r0;

    if ((f & NUMERATOR_MASK) != 0) {
      Unpacked numOut = (f & DENOMINATOR_MASK) != 0 ? first : r0;
      boolean alt = (signFlags & ALTERNATE_SIGN) != 0;
      evalPoly(polyArg, table, coefPtr, degree, alt, numOut, status, scratch);
      if ((f & POST_MULTIPLY) != 0) {
        Unpacked tmp = scratch.unpacked(2);
        UxOps.mulUnpacked(argument, numOut, tmp, status);
        numOut.copyFrom(tmp);
      }
      numOut.exponent += UxTable.readCoefScale(table, coefPtr, degree);
      coefPtr += bankBytes;
      first = numOut;
    } else {
      second = r0;
      f |= NO_DIVIDE;
      if ((f & SKIP) != 0) {
        coefPtr += bankBytes;
      }
    }

    if ((f & DENOMINATOR_MASK) != 0) {
      boolean alt = (signFlags & denominatorFlags(ALTERNATE_SIGN)) != 0;
      evalPoly(polyArg, table, coefPtr, degree, alt, second, status, scratch);
      if ((f & denominatorFlags(POST_MULTIPLY)) != 0) {
        Unpacked tmp = scratch.unpacked(2);
        UxOps.mulUnpacked(argument, second, tmp, status);
        second.copyFrom(tmp);
      }
      second.exponent += UxTable.readCoefScale(table, coefPtr, degree);
      coefPtr += bankBytes;
      if ((f & SKIP) != 0) {
        // Numerator was skipped; den already in second (== r0).
        if (r0 != second) {
          r0.copyFrom(second);
        }
        return;
      }
    } else {
      f |= NO_DIVIDE;
    }

    if ((f & NO_DIVIDE) == 0) {
      // Always result[0] / result[1] (SWAP places den in [0], num in [1]).
      Unpacked quot = scratch.unpacked(2);
      UxOps.divUnpacked(r0, r1, quot, status, scratch.division);
      r0.copyFrom(quot);
    }
  }

  /** Single-result overload (numerator-only / in-place rational). */
  static void evaluateRational(
      Unpacked argument,
      long[] table,
      int coefsOffset,
      int degree,
      long flags,
      Unpacked result,
      StatusFlags status) {
    UxScratch.Frame scratch = UxScratch.acquire();
    try {
      evaluateRationalCore(
          argument, table, coefsOffset, degree, flags,
          result, scratch.unpacked(0), status, scratch);
    } finally {
      UxScratch.release(scratch);
    }
  }

  /**
   * {@code EVALUATE_PACKED_POLY} ({@code UNPACK_COEF_TO_UX} in
   * {@code dpml_ux_ops.c}).
   */
  static void evaluatePackedPoly(
      Unpacked argument,
      long[] table,
      int coefsOffset,
      int degree,
      long mask,
      int bias,
      Unpacked result,
      StatusFlags status) {
    UxScratch.Frame scratch = UxScratch.acquire();
    try {
      int metadata = unpackCoefToUx(table, coefsOffset, mask, bias, result);
      result.sign = (metadata & 1) == ADD ? 0 : Unpacked.UX_SIGN_BIT;
      result.exponent = metadata >> 1;

      Unpacked tmp = scratch.unpacked(0);
      Unpacked term = scratch.unpacked(1);
      Unpacked sum = scratch.unpacked(2);
      int off = coefsOffset;
      int remaining = degree;
      while (--remaining >= 0) {
        UxOps.mulUnpacked(argument, result, tmp, status);
        UxOps.normalize(tmp);
        result.copyFrom(tmp);
        off += UxTable.FIXED_128_BYTES;
        metadata = unpackCoefToUx(table, off, mask, bias, term);
        term.sign = 0;
        term.exponent = 0;
        if ((metadata & 1) == ADD) {
          UxOps.addsubUnpacked(result, term, sum, status);
        } else {
          term.sign = Unpacked.UX_SIGN_BIT;
          UxOps.addsubUnpacked(result, term, sum, status);
        }
        result.copyFrom(sum);
        result.exponent += metadata >> 1;
      }
    } finally {
      UxScratch.release(scratch);
    }
  }

  /**
   * Unpack one packed coefficient: MSD from {@code digits[1]}, LSD from
   * {@code digits[0] & ~mask}; {@code opOut} is {@link #ADD}/{@link #SUB};
   * {@code scaleOut} is {@code ((lsd >> 1) & mask) - bias}.
   */
  private static int unpackCoefToUx(
      long[] table,
      int byteOffset,
      long mask,
      int bias,
      Unpacked dest) {
    int i = byteOffset >>> 3;
    long lsd = table[i];
    long msd = table[i + 1];
    int operation = (int) (lsd & 1L);
    int scale = (int) (((lsd >>> 1) & mask) - (long) bias);
    long fracLo = lsd & ~mask;
    if (msd == 0L && fracLo == 0L) {
      dest.setZero(0);
    } else {
      dest.setNorm(0, 0, msd, fracLo);
    }
    return (scale << 1) | operation;
  }

  /**
   * Horner over a FIXED_128 bank stored high-degree first:
   * {@code c[0]..c[degree]} then a trailing scale word (applied by caller).
   * Matches {@code __eval_pos_poly} coefficient walk order.
   */
  private static void evalPoly(
      Unpacked polyArg,
      long[] table,
      int coefsOffset,
      int degree,
      boolean alternateSign,
      Unpacked result,
      StatusFlags status,
      UxScratch.Frame scratch) {
    Unpacked acc = scratch.unpacked(3);
    Unpacked tmp = scratch.unpacked(4);
    Unpacked coef = scratch.unpacked(5);
    UxTable.fixed128ToUnpacked(table, coefsOffset, acc);
    for (int k = 1; k <= degree; k++) {
      UxOps.mulUnpacked(acc, polyArg, tmp, status);
      UxOps.normalize(tmp);
      UxTable.fixed128ToUnpacked(
          table, coefsOffset + k * UxTable.FIXED_128_BYTES, coef);
      if (alternateSign) {
        tmp.sign ^= Unpacked.UX_SIGN_BIT;
        UxOps.addsubUnpacked(coef, tmp, acc, status);
      } else {
        UxOps.addsubUnpacked(coef, tmp, acc, status);
      }
      UxOps.normalize(acc);
    }
    result.copyFrom(acc);
  }
}
