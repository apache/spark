/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.bidfp.binary128.tables.LogX;
import org.junit.jupiter.api.Test;

/**
 * Focused checks for {@link UxTable} / {@link UxEval} / {@link KernelEval}.
 */
public class KernelEvalTest {

  @Test
  void logXUxOneAndLn2Decode() {
    Unpacked one = KernelEval.readUxFloat(LogX.TABLE, LogX.UX_ONE);
    assertEquals(0, one.sign);
    assertEquals(1, one.exponent);
    assertEquals(Unpacked.UX_MSB, one.fracHi);
    assertEquals(0L, one.fracLo);

    Unpacked ln2 = KernelEval.readUxFloat(LogX.TABLE, LogX.LN_2);
    assertEquals(0, ln2.sign);
    assertEquals(0, ln2.exponent);
    assertEquals(0xB17217F7D1CF79ABL, ln2.fracHi);
    assertEquals(0xC9E3B39803F2F6AFL, ln2.fracLo);
  }

  @Test
  void coefBankBytesAndScale() {
    assertEquals(24, KernelEval.coefBankBytes(0));
    assertEquals(40, KernelEval.coefBankBytes(1));
    assertEquals(3, UxEval.getScale(UxEval.packScale(3)));
    assertEquals(
        UxEval.POST_MULTIPLY << 4,
        UxEval.denominatorFlags(UxEval.POST_MULTIPLY));
  }

  @Test
  void fixed128HalfBecomesOneAfterScale() {
    // FIXED_128 {lo=0, hi=MSB} at exp 0 is 1/2; scale +1 -> 1.
    long[] bank = new long[] {
        0L, Unpacked.UX_MSB, // digits[0]=lo, digits[1]=hi
        1L // trailing scale word
    };
    Unpacked u = new Unpacked();
    KernelEval.fixed128ToUnpacked(bank, 0, u);
    assertEquals(Unpacked.UX_MSB, u.fracHi);
    assertEquals(0, u.exponent);
    u.exponent += KernelEval.readCoefScale(bank, 0, 0);
    assertEquals(1, u.exponent);
  }

  @Test
  void evaluateRationalConstantOne() {
    // degree 0 numerator: FIXED_128 half + scale 1 => 1.0
    long[] table = new long[] {0L, Unpacked.UX_MSB, 1L};
    Unpacked arg = KernelEval.fromInt(0);
    Unpacked out = new Unpacked();
    StatusFlags st = new StatusFlags();
    long flags = UxEval.STANDARD | UxEval.NO_DIVIDE;
    KernelEval.evaluateRational(arg, table, 0, 0, flags, out, st);
    assertEquals(0, out.sign);
    assertEquals(1, out.exponent);
    assertEquals(Unpacked.UX_MSB, out.fracHi);
  }

  @Test
  void evaluateRationalLinearOnePlusX() {
    // Memory [c0, c1, scale]: c0=c1=half (0.5), scale=1 => 0.5*x + 0.5, then
    // *2 => x + 1. Argument x = 1 (UX_ONE).
    long[] table = new long[] {
        0L, Unpacked.UX_MSB,
        0L, Unpacked.UX_MSB,
        1L
    };
    Unpacked arg = KernelEval.readUxFloat(LogX.TABLE, LogX.UX_ONE);
    Unpacked out = new Unpacked();
    StatusFlags st = new StatusFlags();
    long flags = UxEval.STANDARD | UxEval.NO_DIVIDE;
    KernelEval.evaluateRational(arg, table, 0, 1, flags, out, st);
    // Expect 2.0: exp=2, frac=MSB.
    assertEquals(0, out.sign);
    assertEquals(2, out.exponent);
    assertEquals(Unpacked.UX_MSB, out.fracHi);
  }

  @Test
  void evaluatePackedPolyDegreeZero() {
    // Packed coef: digits[1]=MSB, digits[0] encodes op=ADD, scale=1, bias=0.
    // mask covers low bits used for op+scale; leave MSD intact.
    long mask = 0x3FL; // bits [5:0]
    int bias = 0;
    long lsd = (1L << 1) | 0L; // scale=1, op=ADD
    long[] table = new long[] {lsd, Unpacked.UX_MSB};
    Unpacked arg = KernelEval.fromInt(1);
    Unpacked out = new Unpacked();
    StatusFlags st = new StatusFlags();
    KernelEval.evaluatePackedPoly(arg, table, 0, 0, mask, bias, out, st);
    assertEquals(0, out.sign);
    assertEquals(1, out.exponent);
    assertEquals(Unpacked.UX_MSB, out.fracHi);
  }

  @Test
  void packScaleUsesHighBits() {
    long f = UxEval.STANDARD | UxEval.packScale(3);
    assertTrue((f >>> 58) == 3L);
    assertEquals(3, KernelEval.getScale(f));
  }
}
