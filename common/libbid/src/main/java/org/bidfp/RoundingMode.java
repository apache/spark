/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

/** IEEE 754 rounding-direction attributes used by the Intel BID implementation. */
public enum RoundingMode {
  TIES_TO_EVEN,
  TOWARD_NEGATIVE,
  TOWARD_POSITIVE,
  TOWARD_ZERO,
  TIES_AWAY;

  /** Intel RDFP rounding code: 0..4, matching {@code _IDEC_round}. */
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
}
