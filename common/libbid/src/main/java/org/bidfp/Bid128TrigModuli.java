/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the conditions in LICENSE-INTEL are met.
 */
package org.bidfp;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/** Intel bid_sin_table from bid128_sin.c (10^a / 2pi mod 1, 384-bit). */
final class Bid128TrigModuli {
  static final int ROWS = 6147;
  static final long[][] WORDS = load();

  private Bid128TrigModuli() {}

  private static long[][] load() {
    String path = "org/bidfp/bid128_sin_moduli.bin";
    try (InputStream in = Bid128TrigModuli.class.getClassLoader()
            .getResourceAsStream(path)) {
      if (in == null) {
        throw new IllegalStateException("missing " + path);
      }
      DataInputStream data = new DataInputStream(in);
      long[][] words = new long[ROWS][6];
      for (int i = 0; i < ROWS; i++) {
        for (int j = 0; j < 6; j++) {
          words[i][j] = data.readLong();
        }
      }
      return words;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
