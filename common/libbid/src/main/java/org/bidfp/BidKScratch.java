/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bidfp;

/**
 * Per-thread reusable storage for generic BID-k arithmetic.
 *
 * <p>Frames are acquired in LIFO order so nested kernels use distinct
 * {@link DecNum}, {@link UIntLimbs}, and Knuth workspace instances.
 */
final class BidKScratch {
  private static final ThreadLocal<Pool> LOCAL = ThreadLocal.withInitial(Pool::new);

  private BidKScratch() {
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
    final DecNum a = new DecNum();
    final DecNum b = new DecNum();
    final DecNum r = new DecNum();
    final UIntLimbs coeff = new UIntLimbs();
    final UIntLimbs ac = new UIntLimbs();
    final UIntLimbs bc = new UIntLimbs();
    final UIntLimbs rc = new UIntLimbs();
    boolean an;
    boolean bn;
    int ae;
    int be;
    final boolean[] sticky = {false};
    final boolean[] inexact = {false};
    final boolean[] tmpNeg = {false};
    final int[] tmpExp = {0};
    final long[] words = new long[UIntLimbs.MAX_LIMBS];
    final long[] w0 = new long[LimbMath.WORK];
    final long[] w1 = new long[LimbMath.WORK];
    final long[] w2 = new long[LimbMath.WORK];
    final long[] w3 = new long[LimbMath.WORK];
    final long[] w4 = new long[LimbMath.WORK];
    final long[] w5 = new long[LimbMath.WORK];
    final Wide6 xa = new Wide6();
    final Wide6 xb = new Wide6();
    final Wide6 xr = new Wide6();
    final Wide6 xt = new Wide6();
    final Wide8 ya = new Wide8();
    final Wide8 yb = new Wide8();
    final Wide8 yr = new Wide8();
    final Wide8 yt = new Wide8();
    final Wide8 yw = new Wide8();
    final Wide12 za = new Wide12();
    final Wide12 zb = new Wide12();
    final Wide12 zr = new Wide12();
    final Wide12 zt = new Wide12();
    final Wide12 zw = new Wide12();
    final int[] d0 = new int[LimbMath.DIGITS32];
    final int[] d1 = new int[LimbMath.DIGITS32];
    final int[] d2 = new int[LimbMath.DIGITS32];
    final int[] d3 = new int[LimbMath.DIGITS32];
    private final Pool pool;
    private int index;

    private Frame(Pool pool) {
      this.pool = pool;
    }
  }

  private static final class Pool {
    private Frame[] frames = new Frame[4];
    private int depth;
  }
}
