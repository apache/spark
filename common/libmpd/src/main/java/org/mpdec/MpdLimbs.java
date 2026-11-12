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
    int d = 1;
    while (d < 9 && word >= POW10[d]) {
      d++;
    }
    return d;
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
   * to rWork (same buffer may not alias a/b). Returns quotient length.
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

    // Algorithm D (Knuth), base RADIX. Work buffers are caller-provided.
    int d = 0;
    long vv = vIn[vLen - 1];
    while (vv < Mpd.RADIX / 2L) {
      vv <<= 1;
      d++;
    }
    long[] u = uScratch;
    long[] v = vScratch;
    long[] pq = pqScratch;
    clear(u, 0, uLen + 2);
    int un = shiftLeftBits(uIn, uLen, d, u);
    int vn = shiftLeftBits(vIn, vLen, d, v);
    if (un == uLen) {
      u[un] = 0L;
      un++;
    }
    // Need one extra high limb for the recurrence.
    if (u[un - 1] != 0L || un <= vn) {
      if (un < uLen + 2) {
        u[un] = 0L;
        un++;
      }
    }
    int m = un - vn - 1;
    if (m < 0) {
      m = 0;
    }
    clear(q, 0, m + 1);
    for (int j = m; j >= 0; j--) {
      long uj1 = u[j + vn];
      long uj0 = u[j + vn - 1];
      long qhat;
      if (uj1 >= v[vn - 1]) {
        qhat = Mpd.RADIX - 1L;
      } else {
        qhat = (uj1 * Mpd.RADIX + uj0) / v[vn - 1];
      }
      if (qhat >= Mpd.RADIX) {
        qhat = Mpd.RADIX - 1L;
      }
      // Find largest qhat with qhat*v <= u_slice via binary search (qhat already
      // clamped to RADIX-1, so at most ~30 multiply/compares).
      long lo = 0L;
      long hi = qhat;
      while (lo < hi) {
        long mid = (lo + hi + 1L) >>> 1;
        mulWord(v, vn, mid, pq);
        if (cmpPrefix(u, j, vn + 1, pq, vn + 1) >= 0) {
          lo = mid;
        } else {
          hi = mid - 1L;
        }
      }
      qhat = lo;
      mulWord(v, vn, qhat, pq);
      subPrefix(u, j, vn + 1, pq, vn + 1);
      q[j] = qhat;
    }
    shiftRightBits(u, vn, d, r);
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

  private static int cmpPrefix(
      long[] a, int aOff, int aLen, long[] b, int bLen) {
    for (int i = Math.max(aLen, bLen) - 1; i >= 0; i--) {
      long av = i < aLen ? a[aOff + i] : 0L;
      long bv = i < bLen ? b[i] : 0L;
      if (av != bv) {
        return Long.compare(av, bv);
      }
    }
    return 0;
  }

  private static void subPrefix(
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
  }

  private static int shiftLeftBits(long[] in, int len, int bits, long[] out) {
    if (bits == 0) {
      copy(in, len, out);
      return len;
    }
    long carry = 0L;
    for (int i = 0; i < len; i++) {
      long cur = (in[i] << bits) + carry;
      out[i] = cur % Mpd.RADIX;
      carry = cur / Mpd.RADIX;
    }
    int outLen = len;
    if (carry != 0L) {
      out[outLen++] = carry;
    }
    return outLen;
  }

  private static int shiftRightBits(long[] in, int len, int bits, long[] out) {
    if (bits == 0) {
      copy(in, len, out);
      return normalizeLen(out, len);
    }
    long mask = (1L << bits) - 1L;
    long carry = 0L;
    for (int i = len - 1; i >= 0; i--) {
      long cur = in[i] + carry * Mpd.RADIX;
      out[i] = cur >> bits;
      carry = cur & mask;
    }
    return normalizeLen(out, len);
  }

  static final class Result {
    final int len;
    final int sticky;

    Result(int len, int sticky) {
      this.len = len;
      this.sticky = sticky;
    }
  }
}
