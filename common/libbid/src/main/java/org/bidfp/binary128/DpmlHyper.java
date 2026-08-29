/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

/** Intel QUAD UX hyperbolic path from {@code dpml_ux_exp.c}. */
public final class DpmlHyper {
  private static final int SINH_EVAL =
      UxEval.numeratorFlags(UxEval.SQUARE_TERM | UxEval.POST_MULTIPLY) | UxEval.SKIP;
  private static final int COSH_EVAL =
      UxEval.denominatorFlags(UxEval.SQUARE_TERM) | UxEval.SKIP;
  private static final int TANH_EVAL =
      UxEval.numeratorFlags(UxEval.SQUARE_TERM | UxEval.POST_MULTIPLY)
          | UxEval.denominatorFlags(UxEval.SQUARE_TERM);
  private static final int SINHCOSH_EVAL = TANH_EVAL | UxEval.NO_DIVIDE;

  private DpmlHyper() {
  }

  public static Binary128 sinh(Binary128 x, RoundingMode mode, StatusFlags st) {
    return hyperbolic(x, Function.SINH, mode, st);
  }

  public static Binary128 cosh(Binary128 x, RoundingMode mode, StatusFlags st) {
    return hyperbolic(x, Function.COSH, mode, st);
  }

  public static Binary128 tanh(Binary128 x, RoundingMode mode, StatusFlags st) {
    return hyperbolic(x, Function.TANH, mode, st);
  }

  private static Binary128 hyperbolic(
      Binary128 x, Function function, RoundingMode mode, StatusFlags st) {
    if (x.isNaN()) {
      return DpmlExpHyperKernel.quietNaN(x, st);
    }
    if (x.isInfinite()) {
      if (function == Function.COSH) {
        return Binary128.POSITIVE_INFINITY;
      }
      if (function == Function.TANH) {
        return x.isSigned() ? Binary128.ONE.negate() : Binary128.ONE;
      }
      return x;
    }
    if (x.isZero()) {
      return function == Function.COSH ? Binary128.ONE : x;
    }

    StatusFlags local = new StatusFlags();
    Unpacked argument = UxOps.unpack(x);
    int sign = argument.sign;
    argument.sign = 0;
    DpmlExpHyperKernel.Reduction reduction =
        DpmlExpHyperKernel.reduce(argument, false, local);

    Unpacked result;
    if (reduction.scale == 0) {
      int flags = function == Function.SINH
          ? SINH_EVAL : function == Function.COSH ? COSH_EVAL : TANH_EVAL;
      result = DpmlExpHyperKernel.sinhCoshPolynomial(
          reduction.argument, flags, local)[0];
    } else {
      Unpacked[] pair = DpmlExpHyperKernel.sinhCoshPolynomial(
          reduction.argument, SINHCOSH_EVAL, local);
      Unpacked expPositive = DpmlExpHyperKernel.add(pair[1], pair[0], local);
      Unpacked expNegative = DpmlExpHyperKernel.sub(pair[1], pair[0], local);
      expPositive.exponent += (int) reduction.scale - 1;
      expNegative.exponent -= (int) reduction.scale + 1;

      Unpacked numerator = function == Function.COSH
          ? DpmlExpHyperKernel.add(expPositive, expNegative, local)
          : DpmlExpHyperKernel.sub(expPositive, expNegative, local);
      if (function == Function.TANH) {
        Unpacked denominator =
            DpmlExpHyperKernel.add(expPositive, expNegative, local);
        result = DpmlExpHyperKernel.div(numerator, denominator, local);
      } else {
        result = numerator;
      }
    }
    result.sign = function == Function.COSH ? 0 : sign;
    return UxOps.pack(result, mode, st);
  }

  private enum Function {
    SINH,
    COSH,
    TANH
  }
}
