/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mpdec;

/**
 * Mutable decimal floating-point value: coefficient * 10^exp with separate
 * sign/special flags. Limbs are base {@link #RADIX} ({@code 10^9}), least
 * significant limb first. Capacity is fixed at {@link #MAX_LIMBS}; values
 * requiring more than {@link #MAX_DIGITS} digits are rejected.
 *
 * <p>Layout is intentionally close to libmpdec {@code mpd_t}, without heap
 * growth or FNT paths.
 */
public final class Mpd {
  public static final int RDIGITS = 9;
  public static final long RADIX = 1_000_000_000L;
  /** Hard precision cap for this evaluation port. */
  public static final int MAX_DIGITS = 100;
  public static final int MAX_LIMBS =
      (MAX_DIGITS + RDIGITS - 1) / RDIGITS;
  /** Scratch space for multiply/divide intermediates. */
  public static final int WORK_LIMBS = MAX_LIMBS * 4 + 4;

  public static final int SIGN_MASK = 1;
  public static final int INF = 2;
  public static final int NAN = 4;
  public static final int SNAN = 8;
  public static final int SPECIAL = INF | NAN | SNAN;

  /** flags: bit0=negative, INF/NAN/SNAN as above. */
  public int flags;
  /** Exponent of the least significant digit. */
  public int exp;
  /** Number of significant decimal digits in the coefficient. */
  public int digits;
  /** Number of limbs in use in {@link #data}. */
  public int len;
  public final long[] data;

  public Mpd() {
    // WORK_LIMBS so mul/div intermediates fit before rounding.
    this.data = new long[WORK_LIMBS];
  }

  public Mpd(int capacity) {
    if (capacity < 1) {
      throw new IllegalArgumentException("capacity");
    }
    this.data = new long[capacity];
  }

  public static Mpd work() {
    return new Mpd(WORK_LIMBS);
  }

  public boolean isNegative() {
    return (flags & SIGN_MASK) != 0;
  }

  public boolean isSpecial() {
    return (flags & SPECIAL) != 0;
  }

  public boolean isInfinite() {
    return (flags & INF) != 0;
  }

  public boolean isNan() {
    return (flags & (NAN | SNAN)) != 0;
  }

  public boolean isSnan() {
    return (flags & SNAN) != 0;
  }

  public boolean isZero() {
    return !isSpecial() && digits == 0;
  }

  public void setZero(boolean negative) {
    flags = negative ? SIGN_MASK : 0;
    exp = 0;
    digits = 0;
    len = 1;
    data[0] = 0L;
  }

  public void setInfinite(boolean negative) {
    flags = INF | (negative ? SIGN_MASK : 0);
    exp = 0;
    digits = 0;
    len = 1;
    data[0] = 0L;
  }

  public void setNan(boolean signaling) {
    flags = signaling ? SNAN : NAN;
    exp = 0;
    digits = 0;
    len = 1;
    data[0] = 0L;
  }

  public void clearFinite() {
    flags = 0;
    exp = 0;
    digits = 0;
    len = 1;
    data[0] = 0L;
  }

  public void copyFrom(Mpd src) {
    flags = src.flags;
    exp = src.exp;
    digits = src.digits;
    len = src.len;
    System.arraycopy(src.data, 0, data, 0, src.len);
  }

  public Mpd copy() {
    Mpd out = new Mpd(data.length);
    out.copyFrom(this);
    return out;
  }

  /** Adjusted exponent of the most significant digit: exp + digits - 1. */
  public int adjExp() {
    if (isSpecial() || digits == 0) {
      return exp;
    }
    return exp + digits - 1;
  }

  @Override
  public String toString() {
    return MpdFormat.format(this);
  }
}
