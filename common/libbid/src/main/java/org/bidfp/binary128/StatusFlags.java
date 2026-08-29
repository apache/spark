/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

/** Mutable IEEE 754 status flags for an explicit operation context. */
public final class StatusFlags {
  public static final int INVALID = 0x01;
  public static final int DENORMAL = 0x02;
  public static final int DIVIDE_BY_ZERO = 0x04;
  public static final int OVERFLOW = 0x08;
  public static final int UNDERFLOW = 0x10;
  public static final int INEXACT = 0x20;

  private int bits;

  public int bits() {
    return bits;
  }

  public boolean contains(int flag) {
    return (bits & flag) != 0;
  }

  public void raise(int flags) {
    bits |= flags;
  }

  public void raise(StatusFlags other) {
    bits |= other.bits;
  }

  public void clear() {
    bits = 0;
  }
}
