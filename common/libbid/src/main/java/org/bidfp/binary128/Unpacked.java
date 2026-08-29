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
 * DPML {@code UX_FLOAT}: sign, unbiased UX exponent, 128-bit fraction.
 *
 * <p>Normalized finite values store the hidden bit at fraction bit 127 so the
 * fraction lies in {@code [2^127, 2^128)}. The numeric value is
 * {@code +/- (fraction / 2^128) * 2^exponent}, matching Intel's 64-bit UX path
 * ({@code dpml_ux.h}). Zeros use {@link #UX_ZERO_EXPONENT}; infinities use
 * {@link #UX_INFINITY_EXPONENT}.
 */
public final class Unpacked {
  /** Intel {@code UX_ZERO_EXPONENT}: {@code -1 << (F_EXP_WIDTH + 2)}. */
  public static final int UX_ZERO_EXPONENT = -1 << 17;
  /** Intel {@code UX_INFINITY_EXPONENT}. */
  public static final int UX_INFINITY_EXPONENT = -UX_ZERO_EXPONENT - 1;
  public static final long UX_MSB = 0x8000_0000_0000_0000L;
  public static final int UX_SIGN_BIT = 1 << 31;

  static final int CLASS_NORM = 0;
  static final int CLASS_ZERO = 1;
  static final int CLASS_INF = 2;
  static final int CLASS_NAN = 3;

  int sign;
  int exponent;
  long fracHi;
  long fracLo;
  int klass;
  boolean signaling;

  public Unpacked() {
  }

  public Unpacked copy() {
    Unpacked u = new Unpacked();
    u.sign = sign;
    u.exponent = exponent;
    u.fracHi = fracHi;
    u.fracLo = fracLo;
    u.klass = klass;
    u.signaling = signaling;
    return u;
  }

  void copyFrom(Unpacked o) {
    sign = o.sign;
    exponent = o.exponent;
    fracHi = o.fracHi;
    fracLo = o.fracLo;
    klass = o.klass;
    signaling = o.signaling;
  }

  public int signBit() {
    return sign;
  }

  public int exponent() {
    return exponent;
  }

  public long fractionHigh() {
    return fracHi;
  }

  public long fractionLow() {
    return fracLo;
  }

  public boolean isZero() {
    return klass == CLASS_ZERO;
  }

  public boolean isInfinite() {
    return klass == CLASS_INF;
  }

  public boolean isNaN() {
    return klass == CLASS_NAN;
  }

  public boolean isSignalingNaN() {
    return klass == CLASS_NAN && signaling;
  }

  public boolean isFinite() {
    return klass == CLASS_NORM || klass == CLASS_ZERO;
  }

  void setZero(int signBit) {
    sign = signBit;
    exponent = UX_ZERO_EXPONENT;
    fracHi = 0L;
    fracLo = 0L;
    klass = CLASS_ZERO;
    signaling = false;
  }

  void setInf(int signBit) {
    sign = signBit;
    exponent = UX_INFINITY_EXPONENT;
    fracHi = UX_MSB;
    fracLo = 0L;
    klass = CLASS_INF;
    signaling = false;
  }

  void setNaN(boolean signalingNan) {
    sign = 0;
    exponent = UX_INFINITY_EXPONENT;
    fracHi = UX_MSB | 0x4000_0000_0000_0000L;
    fracLo = 0L;
    klass = CLASS_NAN;
    signaling = signalingNan;
  }

  void setNorm(int signBit, int exp, long hi, long lo) {
    sign = signBit;
    exponent = exp;
    fracHi = hi;
    fracLo = lo;
    klass = CLASS_NORM;
    signaling = false;
  }
}
