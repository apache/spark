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
 * Unrolled unsigned 512-bit value (eight little-endian {@code long}s).
 * Decimal256 coefficients fit in four limbs; products need up to eight.
 */
final class Wide8 {
  static final int POW10_MAX = 150;
  private static final long MASK32 = 0xffff_ffffL;
  private static final Wide8[] POW10 = buildPow10();

  long l0;
  long l1;
  long l2;
  long l3;
  long l4;
  long l5;
  long l6;
  long l7;
  /** Ninth limb used as Knuth D overflow; zero outside {@link #divide}. */
  private long extra;

  void clear() {
    l0 = 0L;
    l1 = 0L;
    l2 = 0L;
    l3 = 0L;
    l4 = 0L;
    l5 = 0L;
    l6 = 0L;
    l7 = 0L;
    extra = 0L;
  }

  void copyFrom(Wide8 other) {
    l0 = other.l0;
    l1 = other.l1;
    l2 = other.l2;
    l3 = other.l3;
    l4 = other.l4;
    l5 = other.l5;
    l6 = other.l6;
    l7 = other.l7;
    extra = 0L;
  }

  void copyFrom(Wide12 other) {
    if (other.highWord() != 0L) {
      throw new ArithmeticException("Wide12 value exceeds Wide8");
    }
    l0 = other.limb(0);
    l1 = other.limb(1);
    l2 = other.limb(2);
    l3 = other.limb(3);
    l4 = other.limb(4);
    l5 = other.limb(5);
    l6 = other.limb(6);
    l7 = other.limb(7);
    extra = 0L;
  }

  void copyFrom(UIntLimbs coeff) {
    l0 = coeff.limb(0);
    l1 = coeff.limb(1);
    l2 = coeff.limb(2);
    l3 = coeff.limb(3);
    l4 = 0L;
    l5 = 0L;
    l6 = 0L;
    l7 = 0L;
    extra = 0L;
  }

  boolean isZero() {
    return (l0 | l1 | l2 | l3 | l4 | l5 | l6 | l7) == 0L;
  }

  int compare(Wide8 other) {
    int cmp = Long.compareUnsigned(l7, other.l7);
    if (cmp != 0) {
      return cmp;
    }
    cmp = Long.compareUnsigned(l6, other.l6);
    if (cmp != 0) {
      return cmp;
    }
    cmp = Long.compareUnsigned(l5, other.l5);
    if (cmp != 0) {
      return cmp;
    }
    cmp = Long.compareUnsigned(l4, other.l4);
    if (cmp != 0) {
      return cmp;
    }
    cmp = Long.compareUnsigned(l3, other.l3);
    if (cmp != 0) {
      return cmp;
    }
    cmp = Long.compareUnsigned(l2, other.l2);
    if (cmp != 0) {
      return cmp;
    }
    cmp = Long.compareUnsigned(l1, other.l1);
    return cmp != 0 ? cmp : Long.compareUnsigned(l0, other.l0);
  }

  void add(Wide8 a, Wide8 b) {
    long s = a.l0 + b.l0;
    long c = carry(a.l0, s);
    l0 = s;
    s = a.l1 + b.l1;
    long c2 = carry(a.l1, s);
    long t = s + c;
    c = c2 + carry(s, t);
    l1 = t;
    s = a.l2 + b.l2;
    c2 = carry(a.l2, s);
    t = s + c;
    c = c2 + carry(s, t);
    l2 = t;
    s = a.l3 + b.l3;
    c2 = carry(a.l3, s);
    t = s + c;
    c = c2 + carry(s, t);
    l3 = t;
    s = a.l4 + b.l4;
    c2 = carry(a.l4, s);
    t = s + c;
    c = c2 + carry(s, t);
    l4 = t;
    s = a.l5 + b.l5;
    c2 = carry(a.l5, s);
    t = s + c;
    c = c2 + carry(s, t);
    l5 = t;
    s = a.l6 + b.l6;
    c2 = carry(a.l6, s);
    t = s + c;
    c = c2 + carry(s, t);
    l6 = t;
    l7 = a.l7 + b.l7 + c;
  }

  /** Four-limb add for Decimal224/256 coefficients and aligned addends. */
  void add4(Wide8 a, Wide8 b) {
    long s = a.l0 + b.l0;
    long c = carry(a.l0, s);
    l0 = s;
    s = a.l1 + b.l1;
    long c2 = carry(a.l1, s);
    long t = s + c;
    c = c2 + carry(s, t);
    l1 = t;
    s = a.l2 + b.l2;
    c2 = carry(a.l2, s);
    t = s + c;
    c = c2 + carry(s, t);
    l2 = t;
    s = a.l3 + b.l3;
    c2 = carry(a.l3, s);
    t = s + c;
    l3 = t;
    l4 = c2 + carry(s, t);
    l5 = 0L;
    l6 = 0L;
    l7 = 0L;
  }

  void subtract(Wide8 a, Wide8 b) {
    long d = a.l0 - b.l0;
    long br = Long.compareUnsigned(a.l0, b.l0) < 0 ? 1L : 0L;
    l0 = d;
    d = a.l1 - b.l1;
    long b1 = Long.compareUnsigned(a.l1, b.l1) < 0 ? 1L : 0L;
    long t = d - br;
    br = b1 | (Long.compareUnsigned(d, br) < 0 ? 1L : 0L);
    l1 = t;
    d = a.l2 - b.l2;
    b1 = Long.compareUnsigned(a.l2, b.l2) < 0 ? 1L : 0L;
    t = d - br;
    br = b1 | (Long.compareUnsigned(d, br) < 0 ? 1L : 0L);
    l2 = t;
    d = a.l3 - b.l3;
    b1 = Long.compareUnsigned(a.l3, b.l3) < 0 ? 1L : 0L;
    t = d - br;
    br = b1 | (Long.compareUnsigned(d, br) < 0 ? 1L : 0L);
    l3 = t;
    d = a.l4 - b.l4;
    b1 = Long.compareUnsigned(a.l4, b.l4) < 0 ? 1L : 0L;
    t = d - br;
    br = b1 | (Long.compareUnsigned(d, br) < 0 ? 1L : 0L);
    l4 = t;
    d = a.l5 - b.l5;
    b1 = Long.compareUnsigned(a.l5, b.l5) < 0 ? 1L : 0L;
    t = d - br;
    br = b1 | (Long.compareUnsigned(d, br) < 0 ? 1L : 0L);
    l5 = t;
    d = a.l6 - b.l6;
    b1 = Long.compareUnsigned(a.l6, b.l6) < 0 ? 1L : 0L;
    t = d - br;
    br = b1 | (Long.compareUnsigned(d, br) < 0 ? 1L : 0L);
    l6 = t;
    l7 = a.l7 - b.l7 - br;
  }

  /** Four-limb subtract for Decimal224/256 coefficients and aligned addends. */
  void subtract4(Wide8 a, Wide8 b) {
    long d = a.l0 - b.l0;
    long br = Long.compareUnsigned(a.l0, b.l0) < 0 ? 1L : 0L;
    l0 = d;
    d = a.l1 - b.l1;
    long b1 = Long.compareUnsigned(a.l1, b.l1) < 0 ? 1L : 0L;
    long t = d - br;
    br = b1 | (Long.compareUnsigned(d, br) < 0 ? 1L : 0L);
    l1 = t;
    d = a.l2 - b.l2;
    b1 = Long.compareUnsigned(a.l2, b.l2) < 0 ? 1L : 0L;
    t = d - br;
    br = b1 | (Long.compareUnsigned(d, br) < 0 ? 1L : 0L);
    l2 = t;
    l3 = a.l3 - b.l3 - br;
    l4 = 0L;
    l5 = 0L;
    l6 = 0L;
    l7 = 0L;
  }

  /** 4-limb x 4-limb schoolbook into eight limbs. High limbs ignored. */
  void multiply4(Wide8 a, Wide8 b) {
    clear();
    mac(a.l0, b, 0);
    mac(a.l1, b, 1);
    mac(a.l2, b, 2);
    mac(a.l3, b, 3);
  }

  void multiplySmall(long m) {
    long h = umulh(l0, m);
    l0 *= m;
    long t = l1 * m;
    long s = t + h;
    h = umulh(l1, m) + carry(t, s);
    l1 = s;
    t = l2 * m;
    s = t + h;
    h = umulh(l2, m) + carry(t, s);
    l2 = s;
    t = l3 * m;
    s = t + h;
    h = umulh(l3, m) + carry(t, s);
    l3 = s;
    t = l4 * m;
    s = t + h;
    h = umulh(l4, m) + carry(t, s);
    l4 = s;
    t = l5 * m;
    s = t + h;
    h = umulh(l5, m) + carry(t, s);
    l5 = s;
    t = l6 * m;
    s = t + h;
    h = umulh(l6, m) + carry(t, s);
    l6 = s;
    t = l7 * m;
    s = t + h;
    if (umulh(l7, m) != 0L || carry(t, s) != 0L) {
      throw new ArithmeticException("Wide8 overflow");
    }
    l7 = s;
  }

  void addSmall(long addend) {
    long s = l0 + addend;
    long c = carry(l0, s);
    l0 = s;
    if (c == 0L) {
      return;
    }
    s = l1 + 1L;
    c = carry(l1, s);
    l1 = s;
    if (c == 0L) {
      return;
    }
    s = l2 + 1L;
    c = carry(l2, s);
    l2 = s;
    if (c == 0L) {
      return;
    }
    s = l3 + 1L;
    c = carry(l3, s);
    l3 = s;
    if (c == 0L) {
      return;
    }
    s = l4 + 1L;
    c = carry(l4, s);
    l4 = s;
    if (c == 0L) {
      return;
    }
    s = l5 + 1L;
    c = carry(l5, s);
    l5 = s;
    if (c == 0L) {
      return;
    }
    s = l6 + 1L;
    c = carry(l6, s);
    l6 = s;
    if (c == 0L) {
      return;
    }
    l7 += 1L;
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

  int dividePow10(int digits, boolean[] sticky) {
    sticky[0] = false;
    if (digits <= 0) {
      return 0;
    }
    int remaining = digits;
    while (remaining > 9) {
      sticky[0] |= divideSmall32(1_000_000_000L) != 0L;
      remaining -= 9;
    }
    if (remaining == 1) {
      return (int) divideSmall32(10L);
    }
    long pow = 1L;
    for (int i = 0; i < remaining; i++) {
      pow *= 10L;
    }
    long rem = divideSmall32(pow);
    long lead = pow / 10L;
    sticky[0] |= rem % lead != 0L;
    return (int) (rem / lead);
  }

  long divideSmall32(long divisor) {
    long rem = 0L;
    rem = step32(7, divisor, rem);
    rem = step32(6, divisor, rem);
    rem = step32(5, divisor, rem);
    rem = step32(4, divisor, rem);
    rem = step32(3, divisor, rem);
    rem = step32(2, divisor, rem);
    rem = step32(1, divisor, rem);
    return step32(0, divisor, rem);
  }

  int decimalDigits() {
    if (isZero()) {
      return 1;
    }
    int bits = bitLength();
    int digits = (((bits - 1) * 1233) >>> 12) + 1;
    if (digits >= POW10_MAX) {
      digits = POW10_MAX - 1;
    }
    if (compare(POW10[digits]) >= 0) {
      digits++;
    }
    return digits;
  }

  int bitLength() {
    if (l7 != 0L) {
      return 448 + 64 - Long.numberOfLeadingZeros(l7);
    }
    if (l6 != 0L) {
      return 384 + 64 - Long.numberOfLeadingZeros(l6);
    }
    if (l5 != 0L) {
      return 320 + 64 - Long.numberOfLeadingZeros(l5);
    }
    if (l4 != 0L) {
      return 256 + 64 - Long.numberOfLeadingZeros(l4);
    }
    if (l3 != 0L) {
      return 192 + 64 - Long.numberOfLeadingZeros(l3);
    }
    if (l2 != 0L) {
      return 128 + 64 - Long.numberOfLeadingZeros(l2);
    }
    if (l1 != 0L) {
      return 64 + 64 - Long.numberOfLeadingZeros(l1);
    }
    return 64 - Long.numberOfLeadingZeros(l0);
  }

  /**
   * {@code this = floor(u / v)}, {@code u = u % v}. {@code v} is unchanged.
   * {@code vn} holds the normalized divisor.
   */
  void divide(Wide8 u, Wide8 v, Wide8 vn) {
    extra = 0L;
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
      divideSingle(u, v.l0);
      return;
    }
    int s = Long.numberOfLeadingZeros(v.limb(n - 1));
    vn.copyFrom(v);
    vn.shiftLeftBits(s);
    u.shiftLeftBits(s);
    clear();
    for (int j = m - n; j >= 0; j--) {
      long ujn = u.limb(j + n);
      long ujn1 = u.limb(j + n - 1);
      long ujn2 = j + n >= 2 ? u.limb(j + n - 2) : 0L;
      long vn1 = vn.limb(n - 1);
      long vn2 = vn.limb(n - 2);
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
      setLimb(j, qhat);
    }
    u.shiftRightBits(s);
  }

  void setBit(int index) {
    if (index < 0 || index >= 512) {
      throw new IllegalArgumentException("bit index out of range");
    }
    int word = index >>> 6;
    setLimb(word, limb(word) | (1L << (index & 63)));
  }

  void shiftRightOne() {
    l0 = (l0 >>> 1) | (l1 << 63);
    l1 = (l1 >>> 1) | (l2 << 63);
    l2 = (l2 >>> 1) | (l3 << 63);
    l3 = (l3 >>> 1) | (l4 << 63);
    l4 = (l4 >>> 1) | (l5 << 63);
    l5 = (l5 >>> 1) | (l6 << 63);
    l6 = (l6 >>> 1) | (l7 << 63);
    l7 >>>= 1;
  }

  /**
   * Heron iteration: {@code guess} becomes {@code floor(sqrt(n))}. {@code n}
   * is unchanged. Returns whether {@code n - guess^2} is nonzero.
   */
  static boolean sqrtFloor(Wide8 n, Wide8 guess, Wide8 quot, Wide8 tmp, Wide8 vn) {
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
        tmp.multiply4(guess, guess);
        vn.subtract(n, tmp);
        return !vn.isZero();
      }
      guess.copyFrom(vn);
    }
  }

  void store(long[] dest) {
    LimbMath.clear(dest);
    dest[0] = l0;
    dest[1] = l1;
    dest[2] = l2;
    dest[3] = l3;
    dest[4] = l4;
    dest[5] = l5;
    dest[6] = l6;
    dest[7] = l7;
  }

  void load(long[] src) {
    l0 = src[0];
    l1 = src.length > 1 ? src[1] : 0L;
    l2 = src.length > 2 ? src[2] : 0L;
    l3 = src.length > 3 ? src[3] : 0L;
    l4 = src.length > 4 ? src[4] : 0L;
    l5 = src.length > 5 ? src[5] : 0L;
    l6 = src.length > 6 ? src[6] : 0L;
    l7 = src.length > 7 ? src[7] : 0L;
  }

  String toDecimalString() {
    if (isZero()) {
      return "0";
    }
    Wide8 tmp = new Wide8();
    tmp.copyFrom(this);
    char[] buf = new char[160];
    int pos = buf.length;
    while (!tmp.isZero()) {
      int digit = (int) tmp.divideSmall32(10L);
      buf[--pos] = (char) ('0' + digit);
    }
    return new String(buf, pos, buf.length - pos);
  }

  private void divideSingle(Wide8 u, long divisor) {
    clear();
    long rem = 0L;
    for (int i = 7; i >= 0; i--) {
      long lo = u.limb(i);
      long q;
      if (rem == 0L) {
        q = Long.divideUnsigned(lo, divisor);
        rem = Long.remainderUnsigned(lo, divisor);
      } else {
        q = Bid64Divide.divide128By64(rem, lo, divisor);
        rem = lo - q * divisor;
      }
      setLimb(i, q);
    }
    u.clear();
    u.l0 = rem;
  }

  private int usedLimbs() {
    if (l7 != 0L) {
      return 8;
    }
    if (l6 != 0L) {
      return 7;
    }
    if (l5 != 0L) {
      return 6;
    }
    if (l4 != 0L) {
      return 5;
    }
    if (l3 != 0L) {
      return 4;
    }
    if (l2 != 0L) {
      return 3;
    }
    if (l1 != 0L) {
      return 2;
    }
    return 1;
  }

  private void shiftLeftBits(int s) {
    if (s == 0) {
      extra = 0L;
      return;
    }
    int t = 64 - s;
    extra = l7 >>> t;
    l7 = (l7 << s) | (l6 >>> t);
    l6 = (l6 << s) | (l5 >>> t);
    l5 = (l5 << s) | (l4 >>> t);
    l4 = (l4 << s) | (l3 >>> t);
    l3 = (l3 << s) | (l2 >>> t);
    l2 = (l2 << s) | (l1 >>> t);
    l1 = (l1 << s) | (l0 >>> t);
    l0 <<= s;
  }

  private void shiftRightBits(int s) {
    if (s == 0) {
      extra = 0L;
      return;
    }
    int t = 64 - s;
    l0 = (l0 >>> s) | (l1 << t);
    l1 = (l1 >>> s) | (l2 << t);
    l2 = (l2 >>> s) | (l3 << t);
    l3 = (l3 >>> s) | (l4 << t);
    l4 = (l4 >>> s) | (l5 << t);
    l5 = (l5 >>> s) | (l6 << t);
    l6 = (l6 >>> s) | (l7 << t);
    l7 = (l7 >>> s) | (extra << t);
    extra = 0L;
  }

  private static long subtractMul(Wide8 u, int j, Wide8 v, int n, long qhat) {
    long k = 0L;
    for (int i = 0; i < n; i++) {
      long pLo = qhat * v.limb(i);
      long pHi = umulh(qhat, v.limb(i));
      long ui = u.limb(j + i);
      long t = ui - k;
      long br = Long.compareUnsigned(ui, k) < 0 ? 1L : 0L;
      long t2 = t - pLo;
      br += Long.compareUnsigned(t, pLo) < 0 ? 1L : 0L;
      u.setLimb(j + i, t2);
      k = pHi + br;
    }
    long ui = u.limb(j + n);
    long t = ui - k;
    long br = Long.compareUnsigned(ui, k) < 0 ? 1L : 0L;
    u.setLimb(j + n, t);
    return br;
  }

  private static void addAt(Wide8 u, int j, Wide8 v, int n) {
    long carry = 0L;
    for (int i = 0; i < n; i++) {
      long ui = u.limb(j + i);
      long s = ui + v.limb(i);
      long c = carry(ui, s);
      long t = s + carry;
      c += carry(s, t);
      u.setLimb(j + i, t);
      carry = c;
    }
    long ui = u.limb(j + n);
    u.setLimb(j + n, ui + carry);
  }

  private static boolean productGreater(long a, long b, long hi, long lo) {
    long pHi = umulh(a, b);
    long pLo = a * b;
    int cmp = Long.compareUnsigned(pHi, hi);
    return cmp > 0 || cmp == 0 && Long.compareUnsigned(pLo, lo) > 0;
  }

  private void mac(long m, Wide8 b, int shift) {
    if (m == 0L) {
      return;
    }
    addProduct(shift, m, b.l0);
    addProduct(shift + 1, m, b.l1);
    addProduct(shift + 2, m, b.l2);
    addProduct(shift + 3, m, b.l3);
  }

  private void addProduct(int index, long x, long y) {
    long lo = x * y;
    long hi = umulh(x, y);
    addAt(index, lo);
    if (hi != 0L) {
      addAt(index + 1, hi);
    }
  }

  private void addAt(int index, long value) {
    while (value != 0L) {
      long cur = limb(index);
      long s = cur + value;
      long c = carry(cur, s);
      setLimb(index, s);
      value = c;
      index++;
    }
  }

  private long step32(int index, long divisor, long rem) {
    long value = limb(index);
    long high = value >>> 32;
    long low = value & MASK32;
    long cur = (rem << 32) | high;
    long qh = Long.divideUnsigned(cur, divisor);
    rem = Long.remainderUnsigned(cur, divisor);
    cur = (rem << 32) | low;
    long ql = Long.divideUnsigned(cur, divisor);
    rem = Long.remainderUnsigned(cur, divisor);
    setLimb(index, (qh << 32) | ql);
    return rem;
  }

  private long limb(int index) {
    return switch (index) {
      case 0 -> l0;
      case 1 -> l1;
      case 2 -> l2;
      case 3 -> l3;
      case 4 -> l4;
      case 5 -> l5;
      case 6 -> l6;
      case 7 -> l7;
      case 8 -> extra;
      default -> 0L;
    };
  }

  private void setLimb(int index, long value) {
    switch (index) {
      case 0 -> l0 = value;
      case 1 -> l1 = value;
      case 2 -> l2 = value;
      case 3 -> l3 = value;
      case 4 -> l4 = value;
      case 5 -> l5 = value;
      case 6 -> l6 = value;
      case 7 -> l7 = value;
      case 8 -> extra = value;
      default -> throw new ArithmeticException("Wide8 overflow");
    }
  }

  private static long umulh(long x, long y) {
    return UInt128.unsignedMultiplyHigh(x, y);
  }

  private static long carry(long a, long sum) {
    return Long.compareUnsigned(sum, a) < 0 ? 1L : 0L;
  }

  private static Wide8[] buildPow10() {
    Wide8[] table = new Wide8[POW10_MAX + 1];
    Wide8 value = new Wide8();
    value.l0 = 1L;
    for (int i = 0; i <= POW10_MAX; i++) {
      Wide8 copy = new Wide8();
      copy.copyFrom(value);
      table[i] = copy;
      if (i < POW10_MAX) {
        value.multiplySmall(10L);
      }
    }
    return table;
  }
}
