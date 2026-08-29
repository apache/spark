/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

/** Rounding decision shared by arithmetic, conversion, and quantize kernels. */
final class BidRound {
  private BidRound() {
  }

  /**
   * Whether to round away from zero given the least-significant kept digit
   * {@code coefficient}, the first discarded decimal digit {@code firstDiscarded}
   * (0..9), and whether any remaining discarded digits are nonzero.
   */
  static boolean shouldIncrement(
      boolean negative,
      long coefficient,
      int firstDiscarded,
      boolean sticky,
      RoundingMode mode) {
    boolean inexact = firstDiscarded != 0 || sticky;
    if (!inexact) {
      return false;
    }
    switch (mode) {
      case TOWARD_NEGATIVE:
        return negative;
      case TOWARD_POSITIVE:
        return !negative;
      case TOWARD_ZERO:
        return false;
      case TIES_AWAY:
        return firstDiscarded >= 5;
      case TIES_TO_EVEN:
        return firstDiscarded > 5
            || firstDiscarded == 5 && (sticky || (coefficient & 1L) != 0L);
      default:
        throw new AssertionError(mode);
    }
  }

  static boolean shouldIncrement(
      boolean negative,
      long remainder,
      long divisor,
      long coefficient,
      RoundingMode mode) {
    if (remainder == 0L) {
      return false;
    }
    switch (mode) {
      case TOWARD_NEGATIVE:
        return negative;
      case TOWARD_POSITIVE:
        return !negative;
      case TOWARD_ZERO:
        return false;
      case TIES_AWAY:
        return remainder * 2L >= divisor
            || remainder > divisor - remainder;
      case TIES_TO_EVEN:
        long doubled = remainder * 2L;
        int cmp = Long.compareUnsigned(doubled, divisor);
        if (doubled < remainder) {
          cmp = 1;
        }
        return cmp > 0 || cmp == 0 && (coefficient & 1L) != 0L;
      default:
        throw new AssertionError(mode);
    }
  }

  static boolean overflowToInfinity(boolean negative, RoundingMode mode) {
    switch (mode) {
      case TIES_TO_EVEN:
      case TIES_AWAY:
        return true;
      case TOWARD_POSITIVE:
        return !negative;
      case TOWARD_NEGATIVE:
        return negative;
      case TOWARD_ZERO:
        return false;
      default:
        throw new AssertionError(mode);
    }
  }
}
