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
 * QUAD UX table from Intel {@code dpml_cbrt_x.h}.
 * Little-endian memory image as {@code long[]} (104 bytes).
 */
public final class CbrtX {
  private CbrtX() {
  }

  /** Total table size in bytes (Intel comment offsets). */
  public static final int BYTE_LENGTH = 104;

  public static final int CBRT_CLASS_TO_ACTION_MAP = 0;
  public static final int COEFS = 8;
  public static final int FOURTEEN_NINTHS = 80;
  public static final int POW_CBRT_2_TABLE = 56;
  public static final int SEVEN_NINTHS = 88;
  public static final int TWO_NINTHS = 96;

  /** Little-endian table words (two u32s per long). */
  public static final long[] TABLE = {
      0x1410000000410408L, 0x4006ED4D2E803C66L,
      0xC0102E13C6230110L, 0x400C33EEA71AF473L,
      0xBFFC42EFA7679244L, 0x3FDE3D1A896AD7DAL,
      0xBFAAD21E367E9BA1L, 0x3FF0000000000000L,
      0x3FF428A2F98D728BL, 0x3FF965FEA53D6E3DL,
      0x3FF8E38E38E38E39L, 0x3FE8E38E38E38E39L,
      0x3FCC71C71C71C71CL
  };
}
