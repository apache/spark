/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

import org.bidfp.binary128.tables.TrigX;

/**
 * Radian sin/cos/tan (DPML {@code dpml_ux_trig.c}). Decimal Payne-Hanek
 * moduli from {@code bid64_sin.c} are not used here.
 */
public final class DpmlTrig {
  private static final int ODD_POLY_FLAGS =
      UxEval.SQUARE_TERM | UxEval.ALTERNATE_SIGN | UxEval.POST_MULTIPLY;
  private static final int EVEN_POLY_FLAGS =
      UxEval.SQUARE_TERM | UxEval.ALTERNATE_SIGN;
  private static final int SIN_POLY_FLAGS = UxEval.numeratorFlags(ODD_POLY_FLAGS);
  private static final int COS_POLY_FLAGS = UxEval.denominatorFlags(EVEN_POLY_FLAGS);
  private static final Binary128 INFINITY_NAN =
      Binary128.fromRawBits(0xffff_8000_0000_0000L, 0L);

  private DpmlTrig() {
  }

  public static Binary128 sin(Binary128 x, RoundingMode mode, StatusFlags st) {
    return sincos(x, 0, mode, st);
  }

  public static Binary128 cos(Binary128 x, RoundingMode mode, StatusFlags st) {
    return sincos(x, 2, mode, st);
  }

  public static Binary128 tan(Binary128 x, RoundingMode mode, StatusFlags st) {
    Unpacked argument = UxOps.unpack(x);
    Binary128 special = special(x, argument, false, st);
    if (special != null) {
      return special;
    }

    Unpacked reduced = new Unpacked();
    int quadrant = UxRadianReduce.reduce(argument, 0, reduced, st);
    Unpacked numerator = new Unpacked();
    Unpacked denominator = new Unpacked();
    UxEval.evaluateRational(
        reduced,
        TrigX.TABLE,
        TrigX.TANCOT_COEF_ARRAY,
        TrigX.TANCOT_COEF_ARRAY_DEGREE,
        SIN_POLY_FLAGS | COS_POLY_FLAGS | UxEval.NO_DIVIDE,
        new Unpacked[] {numerator, denominator},
        st);
    Unpacked result = new Unpacked();
    if ((quadrant & 1) == 0) {
      UxOps.divUnpacked(numerator, denominator, result, st);
    } else {
      UxOps.divUnpacked(denominator, numerator, result, st);
      UxOps.negate(result);
    }
    return UxOps.pack(result, mode, st);
  }

  private static Binary128 sincos(
      Binary128 x, int octant, RoundingMode mode, StatusFlags st) {
    Unpacked argument = UxOps.unpack(x);
    Binary128 special = special(x, argument, octant != 0, st);
    if (special != null) {
      return special;
    }

    Unpacked reduced = new Unpacked();
    int quadrant = UxRadianReduce.reduce(argument, octant, reduced, st);
    Unpacked result = new Unpacked();
    int flags = (quadrant & 1) == 0
        ? UxEval.SKIP | SIN_POLY_FLAGS
        : UxEval.SKIP | COS_POLY_FLAGS;
    UxEval.evaluateRational(
        reduced,
        TrigX.TABLE,
        TrigX.SINCOS_COEF_ARRAY,
        TrigX.SINCOS_COEF_ARRAY_DEGREE,
        flags,
        result,
        st);
    if ((quadrant & 2) != 0) {
      UxOps.negate(result);
    }
    return UxOps.pack(result, mode, st);
  }

  private static Binary128 special(
      Binary128 packed, Unpacked argument, boolean cosine, StatusFlags st) {
    if (argument.isNaN()) {
      if (argument.signaling) {
        st.raise(StatusFlags.INVALID);
      }
      return Binary128.fromRawBits(
          packed.highBits() | Binary128.QUIET_NAN_BIT, packed.lowBits());
    }
    if (argument.isInfinite()) {
      st.raise(StatusFlags.INVALID);
      return INFINITY_NAN;
    }
    if (argument.isZero()) {
      return cosine ? Binary128.ONE : packed;
    }
    return null;
  }
}
