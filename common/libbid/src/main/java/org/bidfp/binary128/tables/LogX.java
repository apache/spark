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
 * QUAD UX table from Intel {@code dpml_log_x.h}.
 * Little-endian memory image as {@code long[]} (496 bytes).
 */
public final class LogX {
  private LogX() {
  }

  /** Total table size in bytes (Intel comment offsets). */
  public static final int BYTE_LENGTH = 496;

  public static final int LOG10_CLASS_TO_ACTION_MAP = 48;
  public static final int LOG1P_CLASS_TO_ACTION_MAP = 72;
  public static final int LOG2_CLASS_TO_ACTION_MAP = 24;
  public static final int LOG2_COEF_ARRAY = 104;
  public static final int LOG_CLASS_TO_ACTION_MAP = 0;
  public static final int UX_ONE = 400;
  public static final int UX_TWO = 424;
  public static final int I_RECIP_SQRT_2 = 96;
  public static final int I_SQRT_2 = 88;
  public static final int LN_2 = 448;
  public static final int LOG10_2 = 472;
  public static final int LOG2_COEF_ARRAY_DEGREE = 17;
  public static final int ONE_OVER_SQRT_2 = 88;

  /** Little-endian table words (two u32s per long). */
  public static final long[] TABLE = {
      0x1E7AE40E40E50408L, 0x0000000000000034L,
      0x0000000000000035L, 0x1E7AE40E40E50408L,
      0x0000000000000036L, 0x0000000000000037L,
      0x1E7AE40E40E50408L, 0x0000000000000038L,
      0x0000000000000039L, 0x1410410000E50408L,
      0x0000000000000034L, 0xB504F333F9DE6484L,
      0x5A827999FCEF3242L, 0x271EEE7D56DAC09BL,
      0x06CC4D0D2A1966CEL, 0x1BA3468B6F81E43DL,
      0x056711399CAAC22DL, 0xF7CA0B25A20F818FL,
      0x05F8B50232B2540AL, 0x7ADFA93E3F28F8FEL,
      0x065DF4E9CB8D055CL, 0xCE5C4EA3F7891D9DL,
      0x06D6E7804C87D854L, 0xE820F58A9FEB8D1EL,
      0x0762F8145C44B19AL, 0xE8C1F4C0F720BB2CL,
      0x080766BF41DAD530L, 0x80535F751DF3812CL,
      0x08CB27637D59049FL, 0x96E6A1D72C2AC1EBL,
      0x09B81E0FA68AC838L, 0x8C3B0C947DF70971L,
      0x0ADCD64DBA1F8070L, 0xA70095AA11D8754EL,
      0x0C4F9D8B4A67FF05L, 0x64F2A61E05F3CEFEL,
      0x0E347AB4698BB00EL, 0x572DC64D3936B199L,
      0x10C9A84994022D28L, 0x6A80DDD58C4AC6FEL,
      0x1484B13D7C02A8F8L, 0x645C921FA5C4559CL,
      0x1A61762A7ADED93FL, 0x594E6629AE4A965AL,
      0x24EED8A1DF37FCF2L, 0x3F82AA45785F1ACBL,
      0x3D8E13B87407FAE9L, 0xBE87FED0691D3E89L,
      0xB8AA3B295C17F0BBL, 0x0000000000000002L,
      0x0000000100000000L, 0x8000000000000000L,
      0x0000000000000000L, 0x0000000200000000L,
      0x8000000000000000L, 0x0000000000000000L,
      0x0000000000000000L, 0xB17217F7D1CF79ABL,
      0xC9E3B39803F2F6AFL, 0xFFFFFFFF00000000L,
      0x9A209A84FBCFF798L, 0x8F8959AC0B7C9178L
  };
}
