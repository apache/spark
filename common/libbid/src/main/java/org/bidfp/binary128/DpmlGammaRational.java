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
 * Gamma-specific form of Intel's fixed-point rational evaluator.
 *
 * <p>The table coefficients have term-dependent binary scaling. A normal
 * floating-point Horner loop loses that scaling; {@code __eval_pos_poly}
 * applies an initial {@code -degree * exponent} shift and advances it by the
 * polynomial argument's exponent for each coefficient.
 */
final class DpmlGammaRational {
  private DpmlGammaRational() {
  }

  static Unpacked reduced(
      Unpacked argument, long[] table, int offset, int degree, StatusFlags status) {
    Unpacked polyArgument = argument.copy();
    UxOps.normalize(polyArgument);
    int bankBytes = UxTable.coefBankBytes(degree);
    Unpacked numerator = polynomial(
        polyArgument, table, offset, degree, status);
    numerator.exponent += UxTable.readCoefScale(table, offset, degree);
    int denominatorOffset = offset + bankBytes;
    Unpacked denominator = polynomial(
        polyArgument, table, denominatorOffset, degree, status);
    denominator.exponent += UxTable.readCoefScale(table, denominatorOffset, degree);
    Unpacked result = new Unpacked();
    KernelEval.div(numerator, denominator, result, status);
    return result;
  }

  static Unpacked phi(
      Unpacked inverse, long[] table, int offset, int degree, StatusFlags status) {
    Unpacked scaled = inverse.copy();
    scaled.exponent += 3;
    Unpacked squared = new Unpacked();
    KernelEval.mul(scaled, scaled, squared, status);
    UxOps.normalize(squared);

    int bankBytes = UxTable.coefBankBytes(degree);
    Unpacked numerator = polynomial(squared, table, offset, degree, status);
    numerator.exponent += UxTable.readCoefScale(table, offset, degree);
    Unpacked tmp = new Unpacked();
    KernelEval.mul(scaled, numerator, tmp, status);
    numerator.copyFrom(tmp);

    int denominatorOffset = offset + bankBytes;
    Unpacked denominator = polynomial(
        squared, table, denominatorOffset, degree, status);
    denominator.exponent += UxTable.readCoefScale(table, denominatorOffset, degree);
    Unpacked result = new Unpacked();
    KernelEval.div(numerator, denominator, result, status);
    return result;
  }

  private static Unpacked polynomial(
      Unpacked argument,
      long[] table,
      int offset,
      int degree,
      StatusFlags status) {
    int shift = -degree * argument.exponent;
    int exponent = argument.exponent;
    Unpacked significand = argument.copy();
    significand.exponent = 0;
    Unpacked accumulator = coefficient(table, offset, shift);
    Unpacked product = new Unpacked();
    Unpacked sum = new Unpacked();
    for (int k = 1; k <= degree; k++) {
      KernelEval.mul(accumulator, significand, product, status);
      shift += exponent;
      Unpacked coefficient = coefficient(
          table, offset + k * UxTable.FIXED_128_BYTES, shift);
      KernelEval.add(product, coefficient, sum, status);
      accumulator.copyFrom(sum);
    }
    return accumulator;
  }

  private static Unpacked coefficient(long[] table, int offset, int shift) {
    Unpacked coefficient = new Unpacked();
    UxTable.fixed128ToUnpacked(table, offset, coefficient);
    if (!coefficient.isZero()) {
      coefficient.exponent -= shift;
    }
    return coefficient;
  }
}
