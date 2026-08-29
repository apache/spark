/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

import org.bidfp.binary128.tables.InvTrigX;

/** Intel QUAD UX inverse trig kernels ({@code dpml_ux_inv_trig.c}). */
public final class DpmlInvTrig {
  private static final long[] TABLE = InvTrigX.TABLE;
  private static final long ATAN_FLAGS =
      UxEval.numeratorFlags(UxEval.SQUARE_TERM | UxEval.POST_MULTIPLY)
          | UxEval.denominatorFlags(UxEval.SQUARE_TERM)
          | UxEval.packScale(1);
  private static final long ASIN_FLAGS =
      UxEval.numeratorFlags(
          UxEval.SQUARE_TERM | UxEval.POST_MULTIPLY | UxEval.ALTERNATE_SIGN)
          | UxEval.denominatorFlags(UxEval.SQUARE_TERM | UxEval.ALTERNATE_SIGN)
          | UxEval.packScale(1);

  private DpmlInvTrig() {
  }

  public static Binary128 atan(Binary128 x, RoundingMode mode, StatusFlags st) {
    if (x.isNaN()) {
      return quietNaN(x, st);
    }
    if (x.isZero() || x.isSubnormal()) {
      raiseDenormal(x, st);
      return x;
    }
    if (x.isInfinite()) {
      return signedConstant(InvTrigX.UX_PI_OVER_2_INDEX, x.isSigned(), mode, st);
    }
    return atanFinite(UxOps.unpack(x), null, mode, st);
  }

  /**
   * Computes Intel {@code atan2(y, x)}, including its signed-zero and
   * infinite-quadrant conventions.
   */
  public static Binary128 atan2(
      Binary128 y, Binary128 x, RoundingMode mode, StatusFlags st) {
    raiseDenormal(y, st);
    raiseDenormal(x, st);
    if (y.isNaN()) {
      return quietNaN(y, st);
    }
    if (x.isNaN()) {
      return quietNaN(x, st);
    }
    boolean sy = y.isSigned();
    boolean sx = x.isSigned();
    if (y.isInfinite()) {
      if (x.isInfinite()) {
        int offset = sx
            ? InvTrigX.UX_THREE_QUARTERS_PI_INDEX
            : InvTrigX.UX_PI_OVER_4_INDEX;
        return signedConstant(offset, sy, mode, st);
      }
      return signedConstant(InvTrigX.UX_PI_OVER_2_INDEX, sy, mode, st);
    }
    if (x.isInfinite()) {
      if (sx) {
        return signedConstant(InvTrigX.UX_PI_INDEX, sy, mode, st);
      }
      return sy ? Binary128.NEGATIVE_ZERO : Binary128.ZERO;
    }
    if (y.isZero()) {
      if (sx) {
        return signedConstant(InvTrigX.UX_PI_INDEX, sy, mode, st);
      }
      return sy ? Binary128.NEGATIVE_ZERO : Binary128.ZERO;
    }
    if (x.isZero()) {
      return signedConstant(InvTrigX.UX_PI_OVER_2_INDEX, sy, mode, st);
    }
    return atanFinite(UxOps.unpack(y), UxOps.unpack(x), mode, st);
  }

  public static Binary128 asin(Binary128 x, RoundingMode mode, StatusFlags st) {
    if (x.isNaN()) {
      return quietNaN(x, st);
    }
    if (x.isZero() || x.isSubnormal()) {
      raiseDenormal(x, st);
      return x;
    }
    return asinAcos(x, false, mode, st);
  }

  public static Binary128 acos(Binary128 x, RoundingMode mode, StatusFlags st) {
    if (x.isNaN()) {
      return quietNaN(x, st);
    }
    if (x.isZero() || x.isSubnormal()) {
      raiseDenormal(x, st);
      return constant(InvTrigX.UX_PI_OVER_2_INDEX, mode, st);
    }
    return asinAcos(x, true, mode, st);
  }

  private static Binary128 atanFinite(
      Unpacked y, Unpacked xOrNull, RoundingMode mode, StatusFlags st) {
    boolean signY = y.sign != 0;
    boolean signX = xOrNull != null && xOrNull.sign != 0;
    Unpacked ay = y.copy();
    ay.sign = 0;
    Unpacked ax = xOrNull == null
        ? UxTable.readUxFloat(TABLE, InvTrigX.UX_ONE)
        : xOrNull.copy();
    ax.sign = 0;

    int interval;
    Unpacked red = new Unpacked();
    if (compareScaled(ay, ax, -1) < 0) {
      interval = 0;
      UxOps.divUnpacked(ay, ax, red, st);
    } else if (compareScaled(ay, ax, 1) <= 0) {
      interval = 1;
      Unpacked numerator = new Unpacked();
      Unpacked denominator = new Unpacked();
      KernelEval.sub(ay, ax, numerator, st);
      KernelEval.add(ay, ax, denominator, st);
      UxOps.divUnpacked(numerator, denominator, red, st);
    } else {
      interval = 2;
      UxOps.divUnpacked(ax, ay, red, st);
    }

    Unpacked result = new Unpacked();
    UxEval.evaluateRational(
        red,
        TABLE,
        InvTrigX.ATAN_COEF_ARRAY,
        InvTrigX.ATAN_COEF_ARRAY_DEGREE,
        ATAN_FLAGS,
        result,
        st);

    boolean negateKernel;
    int constantIndex;
    if (!signX) {
      constantIndex = interval == 0 ? InvTrigX.UX_ZERO_INDEX
          : interval == 1 ? InvTrigX.UX_PI_OVER_4_INDEX
          : InvTrigX.UX_PI_OVER_2_INDEX;
      negateKernel = interval == 2;
    } else {
      constantIndex = interval == 0 ? InvTrigX.UX_PI_INDEX
          : interval == 1 ? InvTrigX.UX_THREE_QUARTERS_PI_INDEX
          : InvTrigX.UX_PI_OVER_2_INDEX;
      negateKernel = interval != 2;
    }
    if (negateKernel) {
      UxOps.negate(result);
    }
    if (constantIndex != InvTrigX.UX_ZERO_INDEX) {
      Unpacked sum = new Unpacked();
      KernelEval.add(constant(constantIndex), result, sum, st);
      result.copyFrom(sum);
    }
    result.sign = signY ? Unpacked.UX_SIGN_BIT : 0;
    return UxOps.pack(result, mode, st);
  }

  private static Binary128 asinAcos(
      Binary128 x, boolean acos, RoundingMode mode, StatusFlags st) {
    if (x.isInfinite()) {
      st.raise(StatusFlags.INVALID);
      return Binary128.canonicalNaN(true);
    }
    Unpacked argument = UxOps.unpack(x);
    boolean negative = argument.sign != 0;
    argument.sign = 0;
    Unpacked one = UxTable.readUxFloat(TABLE, InvTrigX.UX_ONE);
    int againstOne = compareScaled(argument, one, 0);
    if (againstOne > 0) {
      st.raise(StatusFlags.INVALID);
      return Binary128.canonicalNaN(true);
    }

    boolean high = compareScaled(argument, one, -1) >= 0;
    int exponentIncrement = 0;
    if (high) {
      exponentIncrement = 1;
      if (againstOne == 0) {
        argument.setZero(0);
      } else {
        Unpacked difference = new Unpacked();
        KernelEval.sub(one, argument, difference, st);
        difference.exponent--;
        UxOps.sqrtUnpacked(difference, argument, st);
      }
    }

    Unpacked result = new Unpacked();
    UxEval.evaluateRational(
        argument,
        TABLE,
        InvTrigX.ASIN_COEF_ARRAY,
        InvTrigX.ASIN_COEF_ARRAY_DEGREE,
        ASIN_FLAGS,
        result,
        st);
    result.exponent += exponentIncrement;

    Unpacked combined = new Unpacked();
    if (!acos) {
      if (high) {
        UxOps.negate(result);
        KernelEval.add(constant(InvTrigX.UX_PI_OVER_2_INDEX), result, combined, st);
      } else {
        combined.copyFrom(result);
      }
      combined.sign = negative ? Unpacked.UX_SIGN_BIT : 0;
    } else if (high) {
      if (negative) {
        UxOps.negate(result);
        KernelEval.add(constant(InvTrigX.UX_PI_INDEX), result, combined, st);
      } else {
        combined.copyFrom(result);
      }
    } else {
      if (!negative) {
        UxOps.negate(result);
      }
      KernelEval.add(constant(InvTrigX.UX_PI_OVER_2_INDEX), result, combined, st);
    }
    return UxOps.pack(combined, mode, st);
  }

  /** Compare {@code a} with {@code b * 2^scale} without overflowing. */
  private static int compareScaled(Unpacked a, Unpacked b, int scale) {
    int exponentComparison = Integer.compare(a.exponent, b.exponent + scale);
    if (exponentComparison != 0) {
      return exponentComparison;
    }
    return Wide.cmp128(a.fracHi, a.fracLo, b.fracHi, b.fracLo);
  }

  private static Unpacked constant(int relativeOffset) {
    return UxTable.readUxFloat(
        TABLE, InvTrigX.INV_TRIG_CONS_BASE + relativeOffset);
  }

  private static Binary128 constant(
      int relativeOffset, RoundingMode mode, StatusFlags st) {
    return UxOps.pack(constant(relativeOffset), mode, new StatusFlags());
  }

  private static Binary128 signedConstant(
      int relativeOffset, boolean negative, RoundingMode mode, StatusFlags st) {
    Unpacked value = constant(relativeOffset);
    value.sign = negative ? Unpacked.UX_SIGN_BIT : 0;
    return UxOps.pack(value, mode, new StatusFlags());
  }

  private static Binary128 quietNaN(Binary128 x, StatusFlags st) {
    if (x.isSignalingNaN()) {
      st.raise(StatusFlags.INVALID);
    }
    return Binary128.fromRawBits(
        x.highBits() | Binary128.QUIET_NAN_BIT, x.lowBits());
  }

  private static void raiseDenormal(Binary128 x, StatusFlags st) {
    if (x.isSubnormal()) {
      st.raise(StatusFlags.DENORMAL);
    }
  }
}
