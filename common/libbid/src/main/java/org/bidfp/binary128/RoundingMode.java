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
 * IEEE 754 rounding-direction attributes. Codes 0..4 match Intel {@code _IDEC_round}
 * and the DPML {@code R{Z,P,M,N,V}_BIT_VECTOR} packing used by {@link UxOps}.
 */
public enum RoundingMode {
  TIES_TO_EVEN,
  TOWARD_NEGATIVE,
  TOWARD_POSITIVE,
  TOWARD_ZERO,
  TIES_AWAY;

  /** Intel RDFP rounding code: 0..4. */
  public int toIntel() {
    return ordinal();
  }

  /** Inverse of {@link #toIntel()}. */
  public static RoundingMode fromIntel(int code) {
    RoundingMode[] values = values();
    if (code < 0 || code >= values.length) {
      throw new IllegalArgumentException("Intel rounding code must be in [0, 4]");
    }
    return values[code];
  }

  /**
   * DPML increment bit-vector indexed by {@code 8*S + 4*K + 2*L + R}
   * (see {@code dpml_ux_int.c}).
   */
  int bitVector() {
    switch (this) {
      case TOWARD_ZERO:
        return 0x0000;
      case TOWARD_POSITIVE:
        return 0x00fa;
      case TOWARD_NEGATIVE:
        return 0xfa00;
      case TIES_TO_EVEN:
        return 0xa8a8;
      case TIES_AWAY:
        return 0xaaaa;
      default:
        throw new IllegalStateException();
    }
  }
}
