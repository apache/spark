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

/** Base-10^9 limb helpers. Least-significant limb at index 0. */
final class MpdLimbs {
  private MpdLimbs() {
  }

  static final long[] POW10 = {
      1L, 10L, 100L, 1_000L, 10_000L, 100_000L,
      1_000_000L, 10_000_000L, 100_000_000L, 1_000_000_000L
  };

  static int digitCount(long word) {
    if (word == 0L) {
      return 0;
    }
    // Binary search on POW10[1..9]; word is in [1, RADIX).
    if (word >= 100_000_000L) {
      return 9;
    }
    if (word >= 1_000_000L) {
      return word >= 10_000_000L ? 8 : 7;
    }
    if (word >= 1_000L) {
      return word >= 100_000L ? 6 : (word >= 10_000L ? 5 : 4);
    }
    return word >= 100L ? 3 : (word >= 10L ? 2 : 1);
  }

  static int digitsOf(long[] data, int len) {
    len = normalizeLen(data, len);
    if (len == 1 && data[0] == 0L) {
      return 0;
    }
    return (len - 1) * Mpd.RDIGITS + digitCount(data[len - 1]);
  }

  static int normalizeLen(long[] data, int len) {
    while (len > 1 && data[len - 1] == 0L) {
      len--;
    }
    if (len <= 0) {
      return 1;
    }
    return len;
  }

  static void clear(long[] data, int from, int toExclusive) {
    for (int i = from; i < toExclusive; i++) {
      data[i] = 0L;
    }
  }

  static void copy(long[] src, int srcLen, long[] dst) {
    System.arraycopy(src, 0, dst, 0, srcLen);
  }

  static int cmpAbs(long[] a, int aLen, long[] b, int bLen) {
    aLen = normalizeLen(a, aLen);
    bLen = normalizeLen(b, bLen);
    if (aLen != bLen) {
      return aLen < bLen ? -1 : 1;
    }
    for (int i = aLen - 1; i >= 0; i--) {
      if (a[i] != b[i]) {
        return Long.compare(a[i], b[i]);
      }
    }
    return 0;
  }

  static int addAbs(long[] a, int aLen, long[] b, int bLen, long[] out) {
    int n = Math.max(aLen, bLen);
    long carry = 0L;
    for (int i = 0; i < n; i++) {
      long sum = carry + (i < aLen ? a[i] : 0L) + (i < bLen ? b[i] : 0L);
      if (sum >= Mpd.RADIX) {
        out[i] = sum - Mpd.RADIX;
        carry = 1L;
      } else {
        out[i] = sum;
        carry = 0L;
      }
    }
    int len = n;
    if (carry != 0L) {
      out[len++] = carry;
    }
    return len;
  }

  /** Requires |a| >= |b|. */
  static int subAbs(long[] a, int aLen, long[] b, int bLen, long[] out) {
    long borrow = 0L;
    for (int i = 0; i < aLen; i++) {
      long av = a[i] - borrow;
      long bv = i < bLen ? b[i] : 0L;
      if (av < bv) {
        out[i] = av + Mpd.RADIX - bv;
        borrow = 1L;
      } else {
        out[i] = av - bv;
        borrow = 0L;
      }
    }
    return normalizeLen(out, aLen);
  }

  static int mulAbs(long[] a, int aLen, long[] b, int bLen, long[] out) {
    if (aLen == 1 && bLen == 1) {
      long prod = a[0] * b[0];
      out[0] = prod % Mpd.RADIX;
      long hi = prod / Mpd.RADIX;
      if (hi == 0L) {
        return 1;
      }
      out[1] = hi;
      return 2;
    }
    if (aLen == 1) {
      return mulWordLen(b, bLen, a[0], out);
    }
    if (bLen == 1) {
      return mulWordLen(a, aLen, b[0], out);
    }
    int outLen = aLen + bLen;
    clear(out, 0, Math.min(outLen, out.length));
    for (int i = 0; i < aLen; i++) {
      long carry = 0L;
      long ai = a[i];
      for (int j = 0; j < bLen; j++) {
        long prod = ai * b[j] + out[i + j] + carry;
        out[i + j] = prod % Mpd.RADIX;
        carry = prod / Mpd.RADIX;
      }
      int k = i + bLen;
      while (carry != 0L) {
        long sum = out[k] + carry;
        out[k] = sum % Mpd.RADIX;
        carry = sum / Mpd.RADIX;
        k++;
      }
    }
    return normalizeLen(out, outLen);
  }

  /** out = v * m with m < RADIX; returns length. */
  private static int mulWordLen(long[] v, int vn, long m, long[] out) {
    if (m == 0L) {
      out[0] = 0L;
      return 1;
    }
    long carry = 0L;
    for (int i = 0; i < vn; i++) {
      long prod = v[i] * m + carry;
      out[i] = prod % Mpd.RADIX;
      carry = prod / Mpd.RADIX;
    }
    if (carry != 0L) {
      out[vn] = carry;
      return vn + 1;
    }
    return vn;
  }

  /** Multiply |a| by 10^n into out. */
  static int mulPow10(long[] a, int aLen, int n, long[] out) {
    if (n < 0) {
      throw new IllegalArgumentException("n");
    }
    if (n == 0) {
      copy(a, aLen, out);
      return aLen;
    }
    int limbShift = n / Mpd.RDIGITS;
    int digitShift = n % Mpd.RDIGITS;
    clear(out, 0, limbShift);
    if (digitShift == 0) {
      System.arraycopy(a, 0, out, limbShift, aLen);
      return normalizeLen(out, aLen + limbShift);
    }
    long mul = POW10[digitShift];
    long div = POW10[Mpd.RDIGITS - digitShift];
    long carry = 0L;
    for (int i = 0; i < aLen; i++) {
      long v = a[i];
      out[i + limbShift] = (v % div) * mul + carry;
      carry = v / div;
    }
    int len = aLen + limbShift;
    if (carry != 0L) {
      out[len++] = carry;
    }
    return normalizeLen(out, len);
  }

  /**
   * Divide coefficient by 10^n into out. Returns sticky (1 if discarded
   * digits were non-zero).
   */
  static Result divPow10(long[] a, int aLen, int n, long[] out) {
    if (n <= 0) {
      copy(a, aLen, out);
      return new Result(aLen, 0);
    }
    int digits = digitsOf(a, aLen);
    if (n >= digits) {
      out[0] = 0L;
      return new Result(1, digits == 0 ? 0 : 1);
    }
    int limbShift = n / Mpd.RDIGITS;
    int digitShift = n % Mpd.RDIGITS;
    int sticky = 0;
    for (int i = 0; i < limbShift; i++) {
      if (a[i] != 0L) {
        sticky = 1;
      }
    }
    int remLen = aLen - limbShift;
    if (digitShift == 0) {
      System.arraycopy(a, limbShift, out, 0, remLen);
      return new Result(normalizeLen(out, remLen), sticky);
    }
    long div = POW10[digitShift];
    long mul = POW10[Mpd.RDIGITS - digitShift];
    if ((a[limbShift] % div) != 0L) {
      sticky = 1;
    }
    long carry = a[limbShift] / div;
    for (int i = 1; i < remLen; i++) {
      long v = a[i + limbShift];
      out[i - 1] = carry + (v % div) * mul;
      carry = v / div;
    }
    out[remLen - 1] = carry;
    return new Result(normalizeLen(out, remLen), sticky);
  }

  /**
   * Knuth long division in base RADIX. Writes quotient to q and remainder
   * to r. Work buffers must not alias the inputs. Returns quotient length.
   */
  static int divAbs(
      long[] uIn, int uLen,
      long[] vIn, int vLen,
      long[] q, long[] r,
      long[] uScratch, long[] vScratch, long[] pqScratch) {
    uLen = normalizeLen(uIn, uLen);
    vLen = normalizeLen(vIn, vLen);
    if (vLen == 1 && vIn[0] == 0L) {
      throw new ArithmeticException("division by zero");
    }
    if (cmpAbs(uIn, uLen, vIn, vLen) < 0) {
      q[0] = 0L;
      copy(uIn, uLen, r);
      return 1;
    }
    if (vLen == 1) {
      long rem = 0L;
      for (int i = uLen - 1; i >= 0; i--) {
        long cur = rem * Mpd.RADIX + uIn[i];
        q[i] = cur / vIn[0];
        rem = cur % vIn[0];
      }
      r[0] = rem;
      clear(r, 1, Math.max(uLen, 1));
      return normalizeLen(q, uLen);
    }

    // Algorithm D (Knuth TAOCP 4.3.1) in base RADIX. Normalize by multiplying
    // with d = floor(RADIX / (v_{n-1}+1)) so the leading divisor limb is
    // >= RADIX/2. Bit shifting is NOT valid normalization in a non-binary base.
    long dMul = Mpd.RADIX / (vIn[vLen - 1] + 1L);
    long[] u = uScratch;
    long[] v = vScratch;
    long[] pq = pqScratch;
    int vn;
    int un;
    if (dMul == 1L) {
      copy(vIn, vLen, v);
      vn = vLen;
      copy(uIn, uLen, u);
      un = uLen;
    } else {
      mulWord(vIn, vLen, dMul, v);
      vn = v[vLen] != 0L ? vLen + 1 : vLen;
      mulWord(uIn, uLen, dMul, u);
      un = u[uLen] != 0L ? uLen + 1 : uLen;
    }
    vn = normalizeLen(v, vn);
    un = normalizeLen(u, un);
    // Extra high limb of zero so u[j+vn] is always defined.
    u[un] = 0L;
    un++;
    int m = un - vn - 1;
    if (m < 0) {
      m = 0;
    }
    clear(q, 0, m + 1);
    long v1 = v[vn - 1];
    long v0 = v[vn - 2];
    for (int j = m; j >= 0; j--) {
      long uj1 = u[j + vn];
      long uj0 = u[j + vn - 1];
      long uj2 = u[j + vn - 2];
      long num = uj1 * Mpd.RADIX + uj0;
      long qhat;
      long rhat;
      if (uj1 >= v1) {
        qhat = Mpd.RADIX - 1L;
        rhat = num - qhat * v1;
      } else {
        qhat = num / v1;
        rhat = num - qhat * v1;
      }
      // D3: decrease qhat while qhat*v0 > rhat*B + uj2.
      while (rhat < Mpd.RADIX && qhat * v0 > rhat * Mpd.RADIX + uj2) {
        qhat--;
        rhat += v1;
      }
      mulWord(v, vn, qhat, pq);
      long borrow = subPrefix(u, j, vn + 1, pq, vn + 1);
      if (borrow != 0L) {
        addPrefix(u, j, vn + 1, v, vn);
        qhat--;
      }
      q[j] = qhat;
    }
    // Unnormalize remainder (divide by dMul).
    if (dMul == 1L) {
      System.arraycopy(u, 0, r, 0, vn);
      return normalizeLen(q, m + 1);
    }
    long rem = 0L;
    for (int i = vn - 1; i >= 0; i--) {
      long cur = rem * Mpd.RADIX + u[i];
      r[i] = cur / dMul;
      rem = cur % dMul;
    }
    return normalizeLen(q, m + 1);
  }

  private static void mulWord(long[] v, int vn, long m, long[] out) {
    long carry = 0L;
    for (int i = 0; i < vn; i++) {
      long prod = v[i] * m + carry;
      out[i] = prod % Mpd.RADIX;
      carry = prod / Mpd.RADIX;
    }
    out[vn] = carry;
  }

  /** Subtract b[0..bLen) from a[aOff..); returns final borrow (0 or 1). */
  private static long subPrefix(
      long[] a, int aOff, int aLen, long[] b, int bLen) {
    long borrow = 0L;
    int n = Math.max(aLen, bLen);
    for (int i = 0; i < n; i++) {
      long av = a[aOff + i] - borrow;
      long bv = i < bLen ? b[i] : 0L;
      if (av < bv) {
        a[aOff + i] = av + Mpd.RADIX - bv;
        borrow = 1L;
      } else {
        a[aOff + i] = av - bv;
        borrow = 0L;
      }
    }
    return borrow;
  }

  /** Add b[0..bLen) into a[aOff..); ignores final carry (D6). */
  private static void addPrefix(
      long[] a, int aOff, int aLen, long[] b, int bLen) {
    long carry = 0L;
    int n = Math.max(aLen, bLen);
    for (int i = 0; i < n; i++) {
      long sum = a[aOff + i] + (i < bLen ? b[i] : 0L) + carry;
      if (sum >= Mpd.RADIX) {
        a[aOff + i] = sum - Mpd.RADIX;
        carry = 1L;
      } else {
        a[aOff + i] = sum;
        carry = 0L;
      }
    }
  }



  static final class Result {
    final int len;
    final int sticky;

    Result(int len, int sticky) {
      this.len = len;
      this.sticky = sticky;
    }
  }

  /** Result of shift-right with rounding info: quotient length, round digit, sticky. */
  static final class RoundResult {
    final int len;
    final int roundDigit;
    final int sticky;

    RoundResult(int len, int roundDigit, int sticky) {
      this.len = len;
      this.roundDigit = roundDigit;
      this.sticky = sticky;
    }
  }

  /**
   * Divide coefficient by 10^n into out, returning the quotient length, the
   * most significant discarded digit (the round digit), and a sticky bit (1
   * if any less-significant discarded digit is non-zero). Single pass.
   */
  static RoundResult divPow10Round(long[] a, int aLen, int n, long[] out) {
    if (n <= 0) {
      copy(a, aLen, out);
      return new RoundResult(aLen, 0, 0);
    }
    int digits = digitsOf(a, aLen);
    if (n >= digits) {
      out[0] = 0L;
      int sticky = digits == 0 ? 0 : 1;
      return new RoundResult(1, 0, sticky);
    }
    int limbShift = n / Mpd.RDIGITS;
    int digitShift = n % Mpd.RDIGITS;
    int sticky = 0;
    // Limbs fully below the round-digit limb.
    int roundLimb = (n - 1) / Mpd.RDIGITS;
    for (int i = 0; i < roundLimb && i < aLen; i++) {
      if (a[i] != 0L) {
        sticky = 1;
      }
    }
    int roundDigit;
    int remLen = aLen - limbShift;
    if (digitShift == 0) {
      // Round digit is top digit of limb (limbShift - 1).
      int rl = limbShift - 1;
      if (rl < 0 || rl >= aLen) {
        roundDigit = 0;
      } else {
        roundDigit = (int) ((a[rl] / POW10[Mpd.RDIGITS - 1]) % 10L);
        if ((a[rl] % POW10[Mpd.RDIGITS - 1]) != 0L) {
          sticky = 1;
        }
      }
      System.arraycopy(a, limbShift, out, 0, remLen);
      return new RoundResult(normalizeLen(out, remLen), roundDigit, sticky);
    }
    long div = POW10[digitShift];
    long mul = POW10[Mpd.RDIGITS - digitShift];
    // Round digit is the top digit of the low part of a[limbShift].
    long lowPart = a[limbShift] % div;
    roundDigit = (int) ((lowPart / POW10[digitShift - 1]) % 10L);
    if ((lowPart % POW10[digitShift - 1]) != 0L) {
      sticky = 1;
    }
    long carry = a[limbShift] / div;
    for (int i = 1; i < remLen; i++) {
      long v = a[i + limbShift];
      out[i - 1] = carry + (v % div) * mul;
      carry = v / div;
    }
    out[remLen - 1] = carry;
    return new RoundResult(normalizeLen(out, remLen), roundDigit, sticky);
  }
}
