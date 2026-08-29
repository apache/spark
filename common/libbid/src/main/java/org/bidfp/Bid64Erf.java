/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

import org.bidfp.binary128.Dpml;

/** Intel {@code bid64_erf.c} and {@code bid64_erfc.c} wrapper behavior. */
final class Bid64Erf {
  private static final long ONE = 0x31c0_0000_0000_0001L;
  private static final long ONE_MINUS_ULP = 0x6bf3_86f2_6fc0_ffffL;
  private static final long TWO = 0x31c0_0000_0000_0002L;
  private static final long TWO_MINUS_ULP = 0x2fe7_1afd_498c_ffffL;

  private Bid64Erf() {
  }

  static long erf(long x, RoundingMode mode, StatusFlags flags) {
    long result = BidTranscendental.unary64(x, mode, flags, Dpml::erf);
    if (Bid64.fromRawBits(x).isFinite()
        && mode == RoundingMode.TOWARD_POSITIVE
        && (result & ~Bid64.MASK_SIGN) == ONE) {
      return (result & Bid64.MASK_SIGN) | ONE_MINUS_ULP;
    }
    return result;
  }

  static long erfc(long x, RoundingMode mode, StatusFlags flags) {
    long result = BidTranscendental.unary64(x, mode, flags, Dpml::erfc);
    if (Bid64.fromRawBits(x).isFinite()
        && x < 0L
        && (mode == RoundingMode.TOWARD_NEGATIVE || mode == RoundingMode.TOWARD_ZERO)
        && result == TWO) {
      return TWO_MINUS_ULP;
    }
    return result;
  }
}
