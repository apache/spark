/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

/** Intel {@code bid64_lgamma.c}: {@code lgamma(-Inf) = +Inf}. */
final class Bid64Lgamma {
  private Bid64Lgamma() {
  }

  static long lgamma(long x, RoundingMode mode, StatusFlags flags) {
    Bid64 value = Bid64.fromRawBits(x);
    if (value.isNaN()) {
      return Bid64Log.canonNan(x, flags);
    }
    if (value.isInfinite() && value.isSigned()) {
      return Bid64.MASK_INFINITY;
    }
    return BidTranscendental.unary64(x, mode, flags, org.bidfp.binary128.Dpml::lgamma);
  }
}
