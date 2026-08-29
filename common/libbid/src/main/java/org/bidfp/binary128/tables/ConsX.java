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
 * QUAD UX table from Intel {@code dpml_cons_x.h}.
 * Little-endian memory image as {@code long[]} (160 bytes).
 */
public final class ConsX {
  private ConsX() {
  }

  /** Total table size in bytes (Intel comment offsets). */
  public static final int BYTE_LENGTH = 160;

  public static final int INF = 9;
  public static final int LAST_CONS_INDEX = 10;
  public static final int NINETY = 7;
  public static final int ONE = 1;
  public static final int ONE_EIGHTY = 8;
  public static final int PI = 3;
  public static final int PI_OVER_2 = 4;
  public static final int PI_OVER_4 = 5;
  public static final int THREE_PI_OVER_4 = 6;
  public static final int TWO = 2;
  public static final int ZERO = 0;

  /** Little-endian table words (two u32s per long). */
  public static final long[] TABLE = {
      0x0000000000000000L, 0x0000000000000000L,
      0x0000000000000000L, 0x3FFF000000000000L,
      0x0000000000000000L, 0x4000000000000000L,
      0x8469898CC51701B8L, 0x4000921FB54442D1L,
      0x8469898CC51701B8L, 0x3FFF921FB54442D1L,
      0x8469898CC51701B8L, 0x3FFE921FB54442D1L,
      0x234F272993D1414AL, 0x40002D97C7F3321DL,
      0x0000000000000000L, 0x4005680000000000L,
      0x0000000000000000L, 0x4006680000000000L,
      0x0000000000000000L, 0x7FFF000000000000L
  };
}
