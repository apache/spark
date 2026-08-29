/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in
 * LICENSE-INTEL are met.
 *
 * Generated from Intel RDFP float128 UX table headers. Do not edit by
 * hand; regenerate from the Intel RDFP table sources.
 */
package org.bidfp.binary128.tables;

/**
 * QUAD UX table from Intel {@code dpml_inv_hyper_x.h}.
 * Little-endian memory image as {@code long[]} (112 bytes).
 */
public final class InvHyperX {
  private InvHyperX() {
  }

  /** Total table size in bytes (Intel comment offsets). */
  public static final int BYTE_LENGTH = 112;

  public static final int ACOSH_CLASS_TO_ACTION_MAP = 8;
  public static final int ASINH_CLASS_TO_ACTION_MAP = 0;
  public static final int ATANH_CLASS_TO_ACTION_MAP = 24;
  public static final int UX_LN2 = 88;
  public static final int UX_ONE = 64;
  public static final int SQRT_2_M1_SQR = 56;
  public static final int SQRT_2_OV_4 = 40;
  public static final int THREE_SQRT_2_OV_4 = 48;

  /** Little-endian table words (two u32s per long). */
  public static final long[] TABLE = {
      0x1410410000410408L, 0x1E79E79E40E50408L,
      0x0000000000000002L, 0x1410410000E79408L,
      0x0000000000000005L, 0xB504F333F9DE6484L,
      0x87C3B666FB66CB63L, 0xAFB0CCC06219B7BAL,
      0x0000000100000000L, 0x8000000000000000L,
      0x0000000000000000L, 0x0000000000000000L,
      0xB17217F7D1CF79ABL, 0xC9E3B39803F2F6AFL
  };
}
