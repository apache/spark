/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

/** IEEE 754 decimal floating-point classifications. */
public enum DecimalClass {
  SIGNALING_NAN,
  QUIET_NAN,
  NEGATIVE_INFINITY,
  NEGATIVE_NORMAL,
  NEGATIVE_SUBNORMAL,
  NEGATIVE_ZERO,
  POSITIVE_ZERO,
  POSITIVE_SUBNORMAL,
  POSITIVE_NORMAL,
  POSITIVE_INFINITY;

  public boolean isNormal() {
    return this == NEGATIVE_NORMAL || this == POSITIVE_NORMAL;
  }

  public boolean isSubnormal() {
    return this == NEGATIVE_SUBNORMAL || this == POSITIVE_SUBNORMAL;
  }
}
