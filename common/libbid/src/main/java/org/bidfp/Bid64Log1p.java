/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

import org.bidfp.binary128.Dpml;

/** Intel {@code bid64_log1p.c}: {@code x < -1/2} uses decimal {@code 1+x}. */
final class Bid64Log1p {
  private static final long MINUS_HALF = 0xb1a0_0000_0000_0005L;
  private static final long ONE = 0x31c0_0000_0000_0001L;
  private static final long NAN = 0x7c00_0000_0000_0000L;

  private Bid64Log1p() {
  }

  static long log1p(long x, RoundingMode mode, StatusFlags flags) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isNaN()) {
      return Bid64Log.canonNan(x, flags);
    }
    if (value.quietLess(Bid64.fromRawBits(MINUS_HALF), new StatusFlags())) {
      long y = Bid64Raw.add(x, ONE, mode, flags);
      if (Bid64.fromRawBits(y).isSigned()) {
        flags.raise(StatusFlags.INVALID);
        return NAN;
      }
      return BidTranscendental.unary64(y, mode, flags, Dpml::log);
    }
    return BidTranscendental.unary64(x, mode, flags, Dpml::log1p);
  }
}
