/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the conditions in LICENSE-INTEL
 * are met.
 */
package org.bidfp.binary128;

/**
 * Per-thread reusable storage for short-lived unpacked arithmetic.
 *
 * <p>Frames are acquired in LIFO order. Nested kernels therefore use distinct
 * storage, while sequential calls on the same thread reuse their frames.
 */
final class UxScratch {
  private static final int UNPACKED_COUNT = 8;
  private static final ThreadLocal<Pool> LOCAL = ThreadLocal.withInitial(Pool::new);

  private UxScratch() {
  }

  static Frame acquire() {
    Pool pool = LOCAL.get();
    int index = pool.depth++;
    if (index == pool.frames.length) {
      Frame[] grown = new Frame[pool.frames.length << 1];
      System.arraycopy(pool.frames, 0, grown, 0, pool.frames.length);
      pool.frames = grown;
    }
    Frame frame = pool.frames[index];
    if (frame == null) {
      frame = new Frame(pool);
      pool.frames[index] = frame;
    }
    frame.index = index;
    return frame;
  }

  static void release(Frame frame) {
    Pool pool = frame.pool;
    if (pool.depth == 0 || frame.index != pool.depth - 1) {
      throw new IllegalStateException("scratch frames must be released in LIFO order");
    }
    pool.depth--;
  }

  static final class Frame {
    final Unpacked[] unpacked = new Unpacked[UNPACKED_COUNT];
    final long[] division = new long[5];
    final long[] root = new long[3];
    final long[] remainder = new long[3];
    final long[] trial = new long[3];
    final long[] radianProduct = new long[4];
    private final Pool pool;
    private int index;

    private Frame(Pool pool) {
      this.pool = pool;
      for (int i = 0; i < unpacked.length; i++) {
        unpacked[i] = new Unpacked();
      }
    }

    Unpacked unpacked(int index) {
      return unpacked[index];
    }
  }

  private static final class Pool {
    Frame[] frames = new Frame[2];
    int depth;
  }
}
