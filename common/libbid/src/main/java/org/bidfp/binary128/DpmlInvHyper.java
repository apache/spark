/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

import org.bidfp.binary128.tables.InvHyperX;

/** Faithful QUAD UX inverse-hyperbolic port from Intel {@code dpml_ux_inv_hyper.c}. */
public final class DpmlInvHyper {
  private DpmlInvHyper() {
  }

  public static Binary128 asinh(Binary128 x, RoundingMode mode, StatusFlags st) {
    DpmlLogInvHyperKernel.noteDenormal(x, st);
    if (x.isNaN()) {
      return DpmlLogInvHyperKernel.quietNaN(x, st);
    }
    if (x.isInfinite() || x.isZero() || x.isSubnormal()) {
      return x;
    }

    StatusFlags local = new StatusFlags();
    Unpacked argument = UxOps.unpack(x);
    int sign = argument.sign;
    argument.sign = 0;
    UxOps.normalize(argument);
    Unpacked one = UxTable.readUxFloat(InvHyperX.TABLE, InvHyperX.UX_ONE);
    Unpacked square = DpmlLogInvHyperKernel.multiply(argument, argument, local);
    Unpacked root = DpmlLogInvHyperKernel.sqrt(
        DpmlLogInvHyperKernel.add(square, one, local), local);

    Unpacked result;
    long threshold = UxTable.word64(InvHyperX.TABLE, InvHyperX.SQRT_2_OV_4);
    if (argument.exponent < -1
        || (argument.exponent == -1
            && Long.compareUnsigned(argument.fracHi, threshold) <= 0)) {
      Unpacked denominator = DpmlLogInvHyperKernel.add(root, one, local);
      Unpacked reduced = DpmlLogInvHyperKernel.divide(argument, denominator, local);
      result = DpmlLogInvHyperKernel.logPoly(reduced, local);
    } else {
      Unpacked sum = DpmlLogInvHyperKernel.add(root, argument, local);
      result = DpmlLogInvHyperKernel.log(
          sum, UxTable.readUxFloat(InvHyperX.TABLE, InvHyperX.UX_LN2), local);
    }
    result.sign = sign;
    return UxOps.pack(result, mode, st);
  }

  public static Binary128 acosh(Binary128 x, RoundingMode mode, StatusFlags st) {
    DpmlLogInvHyperKernel.noteDenormal(x, st);
    if (x.isNaN()) {
      return DpmlLogInvHyperKernel.quietNaN(x, st);
    }
    if (x.isInfinite()) {
      return x.isSigned()
          ? DpmlLogInvHyperKernel.invalidNaN(st)
          : Binary128.POSITIVE_INFINITY;
    }
    if (x.isSigned() || x.isZero() || x.isSubnormal()) {
      return DpmlLogInvHyperKernel.invalidNaN(st);
    }

    StatusFlags local = new StatusFlags();
    Unpacked argument = UxOps.unpack(x);
    UxOps.normalize(argument);
    Unpacked one = UxTable.readUxFloat(InvHyperX.TABLE, InvHyperX.UX_ONE);
    Unpacked sum = DpmlLogInvHyperKernel.add(argument, one, local);
    Unpacked difference = DpmlLogInvHyperKernel.subtract(argument, one, local);
    if (difference.sign != 0) {
      return DpmlLogInvHyperKernel.invalidNaN(st);
    }

    Unpacked result;
    long threshold = UxTable.word64(InvHyperX.TABLE, InvHyperX.THREE_SQRT_2_OV_4);
    if (argument.exponent == 1
        && Long.compareUnsigned(argument.fracHi, threshold) <= 0) {
      Unpacked ratio = DpmlLogInvHyperKernel.divide(difference, sum, local);
      result = DpmlLogInvHyperKernel.logPoly(
          DpmlLogInvHyperKernel.sqrt(ratio, local), local);
    } else {
      Unpacked product = DpmlLogInvHyperKernel.multiply(difference, sum, local);
      Unpacked root = DpmlLogInvHyperKernel.sqrt(product, local);
      result = DpmlLogInvHyperKernel.log(
          DpmlLogInvHyperKernel.add(root, argument, local),
          UxTable.readUxFloat(InvHyperX.TABLE, InvHyperX.UX_LN2),
          local);
    }
    return UxOps.pack(result, mode, st);
  }

  public static Binary128 atanh(Binary128 x, RoundingMode mode, StatusFlags st) {
    DpmlLogInvHyperKernel.noteDenormal(x, st);
    if (x.isNaN()) {
      return DpmlLogInvHyperKernel.quietNaN(x, st);
    }
    if (x.isInfinite()) {
      return DpmlLogInvHyperKernel.invalidNaN(st);
    }
    if (x.isZero() || x.isSubnormal()) {
      return x;
    }

    StatusFlags local = new StatusFlags();
    Unpacked argument = UxOps.unpack(x);
    int sign = argument.sign;
    argument.sign = 0;
    UxOps.normalize(argument);
    if (argument.exponent >= 1) {
      if (argument.exponent == 1
          && argument.fracHi == Unpacked.UX_MSB
          && argument.fracLo == 0L) {
        st.raise(StatusFlags.DIVIDE_BY_ZERO);
        return sign == 0 ? Binary128.POSITIVE_INFINITY : Binary128.NEGATIVE_INFINITY;
      }
      return DpmlLogInvHyperKernel.invalidNaN(st);
    }

    Unpacked result;
    long threshold = UxTable.word64(InvHyperX.TABLE, InvHyperX.SQRT_2_M1_SQR);
    if (argument.exponent < -2
        || (argument.exponent == -2
            && Long.compareUnsigned(argument.fracHi, threshold) <= 0)) {
      result = DpmlLogInvHyperKernel.logPoly(argument, local);
    } else {
      Unpacked one = UxTable.readUxFloat(InvHyperX.TABLE, InvHyperX.UX_ONE);
      Unpacked sum = DpmlLogInvHyperKernel.add(argument, one, local);
      Unpacked difference = DpmlLogInvHyperKernel.subtract(one, argument, local);
      Unpacked ratio = DpmlLogInvHyperKernel.divide(sum, difference, local);
      result = DpmlLogInvHyperKernel.log(
          ratio, UxTable.readUxFloat(InvHyperX.TABLE, InvHyperX.UX_LN2), local);
    }
    result.sign = sign;
    result.exponent--;
    return UxOps.pack(result, mode, st);
  }
}
