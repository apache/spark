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
 * Unsigned 768-bit value (twelve little-endian {@code long}s). Decimal256
 * sqrt scales the radicand to ~724 bits, which does not fit in {@link Wide8}.
 */
final class Wide12 {
  static final int LIMBS = 12;
  static final int POW10_MAX = 160;

  private final long[] d = new long[LIMBS + 1];

  void clear() {
    for (int i = 0; i < d.length; i++) {
      d[i] = 0L;
    }
  }

  void copyFrom(Wide12 other) {
    System.arraycopy(other.d, 0, d, 0, LIMBS);
    d[LIMBS] = 0L;
  }

  void copyFrom(UIntLimbs coeff) {
    clear();
    int n = Math.min(LIMBS, UIntLimbs.MAX_LIMBS);
    for (int i = 0; i < n; i++) {
      d[i] = coeff.limb(i);
    }
  }

  boolean isZero() {
    long acc = 0L;
    for (int i = 0; i < LIMBS; i++) {
      acc |= d[i];
    }
    return acc == 0L;
  }

  long limb(int index) {
    return d[index];
  }

  void setLimb(int index, long value) {
    d[index] = value;
  }

  void store(long[] dest) {
    LimbMath.clear(dest);
    System.arraycopy(d, 0, dest, 0, LIMBS);
  }

  long highWord() {
    long acc = d[8];
    for (int i = 9; i < LIMBS; i++) {
      acc |= d[i];
    }
    return acc;
  }

  int compare(Wide12 other) {
    for (int i = LIMBS - 1; i >= 0; i--) {
      int cmp = Long.compareUnsigned(d[i], other.d[i]);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  void add(Wide12 a, Wide12 b) {
    long carry = 0L;
    for (int i = 0; i < LIMBS; i++) {
      long s = a.d[i] + b.d[i];
      long c = carry(a.d[i], s);
      long t = s + carry;
      c += carry(s, t);
      d[i] = t;
      carry = c;
    }
    d[LIMBS] = 0L;
    if (carry != 0L) {
      throw new ArithmeticException("Wide12 overflow");
    }
  }

  void subtract(Wide12 a, Wide12 b) {
    long br = 0L;
    for (int i = 0; i < LIMBS; i++) {
      long diff = a.d[i] - b.d[i];
      long b1 = Long.compareUnsigned(a.d[i], b.d[i]) < 0 ? 1L : 0L;
      long t = diff - br;
      br = b1 | (Long.compareUnsigned(diff, br) < 0 ? 1L : 0L);
      d[i] = t;
    }
    d[LIMBS] = 0L;
  }

  void multiply(Wide12 a, Wide12 b) {
    clear();
    for (int i = 0; i < LIMBS; i++) {
      long m = a.d[i];
      if (m == 0L) {
        continue;
      }
      for (int j = 0; j < LIMBS; j++) {
        int k = i + j;
        if (k > LIMBS) {
          if (b.d[j] != 0L) {
            throw new ArithmeticException("Wide12 overflow");
          }
          continue;
        }
        addProduct(k, m, b.d[j]);
      }
    }
  }

  void multiplySmall(long m) {
    long h = 0L;
    for (int i = 0; i < LIMBS; i++) {
      long t = d[i] * m;
      long s = t + h;
      long nh = umulh(d[i], m) + carry(t, s);
      d[i] = s;
      h = nh;
    }
    d[LIMBS] = 0L;
    if (h != 0L) {
      throw new ArithmeticException("Wide12 overflow");
    }
  }

  void multiplyPow10(int exp) {
    if (exp < 0 || exp > POW10_MAX) {
      throw new IllegalArgumentException("pow10 exponent out of range");
    }
    while (exp >= 9) {
      multiplySmall(1_000_000_000L);
      exp -= 9;
    }
    if (exp > 0) {
      long f = 1L;
      for (int i = 0; i < exp; i++) {
        f *= 10L;
      }
      multiplySmall(f);
    }
  }

  int bitLength() {
    for (int i = LIMBS - 1; i >= 0; i--) {
      if (d[i] != 0L) {
        return i * 64 + (64 - Long.numberOfLeadingZeros(d[i]));
      }
    }
    return 0;
  }

  void setBit(int index) {
    if (index < 0 || index >= LIMBS * 64) {
      throw new IllegalArgumentException("bit index out of range");
    }
    d[index >>> 6] |= 1L << (index & 63);
  }

  void shiftRightOne() {
    for (int i = 0; i < LIMBS - 1; i++) {
      d[i] = (d[i] >>> 1) | (d[i + 1] << 63);
    }
    d[LIMBS - 1] >>>= 1;
    d[LIMBS] = 0L;
  }

  /**
   * {@code this = floor(u / v)}, {@code u = u % v}. {@code v} is unchanged.
   * {@code vn} holds the normalized divisor.
   */
  void divide(Wide12 u, Wide12 v, Wide12 vn) {
    d[LIMBS] = 0L;
    if (v.isZero()) {
      throw new ArithmeticException("division by zero");
    }
    if (u.isZero() || u.compare(v) < 0) {
      clear();
      return;
    }
    int n = v.usedLimbs();
    int m = u.usedLimbs();
    if (n == 1) {
      divideSingle(u, v.d[0]);
      return;
    }
    int s = Long.numberOfLeadingZeros(v.d[n - 1]);
    vn.copyFrom(v);
    vn.shiftLeftBits(s);
    u.shiftLeftBits(s);
    clear();
    for (int j = m - n; j >= 0; j--) {
      long ujn = u.d[j + n];
      long ujn1 = u.d[j + n - 1];
      long ujn2 = j + n >= 2 ? u.d[j + n - 2] : 0L;
      long vn1 = vn.d[n - 1];
      long vn2 = vn.d[n - 2];
      long qhat;
      long rhat;
      boolean rhatOverflow;
      if (ujn == vn1) {
        qhat = -1L;
        rhat = ujn + ujn1;
        rhatOverflow = Long.compareUnsigned(rhat, ujn) < 0;
      } else {
        qhat = Bid64Divide.divide128By64(ujn, ujn1, vn1);
        rhat = ujn1 - qhat * vn1;
        rhatOverflow = false;
      }
      while (!rhatOverflow && productGreater(qhat, vn2, rhat, ujn2)) {
        qhat--;
        long prev = rhat;
        rhat += vn1;
        rhatOverflow = Long.compareUnsigned(rhat, prev) < 0;
      }
      if (subtractMul(u, j, vn, n, qhat) != 0L) {
        qhat--;
        addAt(u, j, vn, n);
      }
      d[j] = qhat;
    }
    u.shiftRightBits(s);
  }

  static boolean sqrtFloor(Wide12 n, Wide12 guess, Wide12 quot, Wide12 tmp, Wide12 vn) {
    if (n.isZero()) {
      guess.clear();
      return false;
    }
    int rootBits = (n.bitLength() + 1) >>> 1;
    guess.clear();
    guess.setBit(rootBits);
    while (true) {
      tmp.copyFrom(n);
      quot.divide(tmp, guess, vn);
      vn.add(quot, guess);
      vn.shiftRightOne();
      if (vn.compare(guess) >= 0) {
        tmp.multiply(guess, guess);
        vn.subtract(n, tmp);
        return !vn.isZero();
      }
      guess.copyFrom(vn);
    }
  }

  private void divideSingle(Wide12 u, long divisor) {
    clear();
    long rem = 0L;
    for (int i = LIMBS - 1; i >= 0; i--) {
      long lo = u.d[i];
      long q;
      if (rem == 0L) {
        q = Long.divideUnsigned(lo, divisor);
        rem = Long.remainderUnsigned(lo, divisor);
      } else {
        q = Bid64Divide.divide128By64(rem, lo, divisor);
        rem = lo - q * divisor;
      }
      d[i] = q;
    }
    u.clear();
    u.d[0] = rem;
  }

  private int usedLimbs() {
    for (int i = LIMBS - 1; i >= 1; i--) {
      if (d[i] != 0L) {
        return i + 1;
      }
    }
    return 1;
  }

  private void shiftLeftBits(int s) {
    if (s == 0) {
      d[LIMBS] = 0L;
      return;
    }
    int t = 64 - s;
    d[LIMBS] = d[LIMBS - 1] >>> t;
    for (int i = LIMBS - 1; i >= 1; i--) {
      d[i] = (d[i] << s) | (d[i - 1] >>> t);
    }
    d[0] <<= s;
  }

  private void shiftRightBits(int s) {
    if (s == 0) {
      d[LIMBS] = 0L;
      return;
    }
    int t = 64 - s;
    for (int i = 0; i < LIMBS; i++) {
      d[i] = (d[i] >>> s) | (d[i + 1] << t);
    }
    d[LIMBS] = 0L;
  }

  private static long subtractMul(Wide12 u, int j, Wide12 v, int n, long qhat) {
    long k = 0L;
    for (int i = 0; i < n; i++) {
      long pLo = qhat * v.d[i];
      long pHi = umulh(qhat, v.d[i]);
      long ui = u.d[j + i];
      long t = ui - k;
      long br = Long.compareUnsigned(ui, k) < 0 ? 1L : 0L;
      long t2 = t - pLo;
      br += Long.compareUnsigned(t, pLo) < 0 ? 1L : 0L;
      u.d[j + i] = t2;
      k = pHi + br;
    }
    long ui = u.d[j + n];
    long t = ui - k;
    long br = Long.compareUnsigned(ui, k) < 0 ? 1L : 0L;
    u.d[j + n] = t;
    return br;
  }

  private static void addAt(Wide12 u, int j, Wide12 v, int n) {
    long carry = 0L;
    for (int i = 0; i < n; i++) {
      long ui = u.d[j + i];
      long s = ui + v.d[i];
      long c = carry(ui, s);
      long t = s + carry;
      c += carry(s, t);
      u.d[j + i] = t;
      carry = c;
    }
    u.d[j + n] += carry;
  }

  private void addProduct(int index, long x, long y) {
    long lo = x * y;
    long hi = umulh(x, y);
    addAtIndex(index, lo);
    if (hi != 0L) {
      addAtIndex(index + 1, hi);
    }
  }

  private void addAtIndex(int index, long value) {
    while (value != 0L) {
      if (index > LIMBS) {
        throw new ArithmeticException("Wide12 overflow");
      }
      long cur = d[index];
      long s = cur + value;
      d[index] = s;
      value = carry(cur, s);
      index++;
    }
  }

  private static boolean productGreater(long a, long b, long hi, long lo) {
    long pHi = umulh(a, b);
    long pLo = a * b;
    int cmp = Long.compareUnsigned(pHi, hi);
    return cmp > 0 || cmp == 0 && Long.compareUnsigned(pLo, lo) > 0;
  }

  private static long umulh(long x, long y) {
    return UInt128.unsignedMultiplyHigh(x, y);
  }

  private static long carry(long a, long sum) {
    return Long.compareUnsigned(sum, a) < 0 ? 1L : 0L;
  }
}
