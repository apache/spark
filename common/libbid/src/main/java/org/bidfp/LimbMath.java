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
 * Little-endian unsigned limb arithmetic for BID-k coefficients.
 *
 * <p>A coefficient of Decimal256 is 70 digits (233 bits), so four {@code long}
 * limbs hold a packed significand. Products, aligned addends, and
 * {@code 10^s} scaled dividends use a 10-limb workspace (640 bits), which
 * covers {@code 10^192}. Schoolbook multiply and Knuth Algorithm D (base
 * 2^32) are the kernels; {@link DecNum} stays the parse/quantize path.
 */
final class LimbMath {
  static final int WORK = 10;
  static final int DIGITS32 = WORK * 2 + 2;
  static final int POW10_MAX = 180;
  private static final long MASK32 = 0xffff_ffffL;
  private static final long[][] POW10 = buildPow10();

  private LimbMath() {
  }

  static void clear(long[] a) {
    for (int i = 0; i < a.length; i++) {
      a[i] = 0L;
    }
  }

  static void copy(long[] dest, long[] src) {
    int n = Math.min(dest.length, src.length);
    System.arraycopy(src, 0, dest, 0, n);
    for (int i = n; i < dest.length; i++) {
      dest[i] = 0L;
    }
  }

  static void copyLimbs(long[] dest, UIntLimbs src) {
    clear(dest);
    int n = Math.min(dest.length, UIntLimbs.MAX_LIMBS);
    for (int i = 0; i < n; i++) {
      dest[i] = src.limb(i);
    }
  }

  static void toUIntLimbs(long[] src, UIntLimbs dest) {
    dest.copyFrom(src);
    for (int i = UIntLimbs.MAX_LIMBS; i < src.length; i++) {
      if (src[i] != 0L) {
        throw new ArithmeticException("coefficient exceeds 256 bits");
      }
    }
  }

  static boolean isZero(long[] a) {
    long acc = 0L;
    for (int i = 0; i < a.length; i++) {
      acc |= a[i];
    }
    return acc == 0L;
  }

  static int compare(long[] a, long[] b) {
    int n = Math.max(a.length, b.length);
    for (int i = n - 1; i >= 0; i--) {
      long left = i < a.length ? a[i] : 0L;
      long right = i < b.length ? b[i] : 0L;
      int cmp = Long.compareUnsigned(left, right);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  static int bitLength(long[] a) {
    for (int i = a.length - 1; i >= 0; i--) {
      if (a[i] != 0L) {
        return i * 64 + 64 - Long.numberOfLeadingZeros(a[i]);
      }
    }
    return 0;
  }

  static int decimalDigits(long[] a) {
    if (isZero(a)) {
      return 1;
    }
    int bits = bitLength(a);
    int digits = (((bits - 1) * 1233) >>> 12) + 1;
    if (digits >= POW10_MAX) {
      digits = POW10_MAX - 1;
    }
    if (compare(a, POW10[digits]) >= 0) {
      digits++;
    }
    return digits;
  }

  /** {@code dest = a + b}. Returns carry (0 or 1). */
  static long add(long[] dest, long[] a, long[] b) {
    long carry = 0L;
    int n = dest.length;
    for (int i = 0; i < n; i++) {
      long av = i < a.length ? a[i] : 0L;
      long bv = i < b.length ? b[i] : 0L;
      long sum = av + bv;
      long c1 = Long.compareUnsigned(sum, av) < 0 ? 1L : 0L;
      long tot = sum + carry;
      long c2 = Long.compareUnsigned(tot, sum) < 0 ? 1L : 0L;
      dest[i] = tot;
      carry = c1 + c2;
    }
    return carry;
  }

  /** {@code dest = a - b}. Requires {@code a >= b}. Returns borrow. */
  static long subtract(long[] dest, long[] a, long[] b) {
    long borrow = 0L;
    int n = dest.length;
    for (int i = 0; i < n; i++) {
      long av = i < a.length ? a[i] : 0L;
      long bv = i < b.length ? b[i] : 0L;
      long sub = av - bv;
      long b1 = Long.compareUnsigned(av, bv) < 0 ? 1L : 0L;
      long tot = sub - borrow;
      long b2 = Long.compareUnsigned(sub, borrow) < 0 ? 1L : 0L;
      dest[i] = tot;
      borrow = b1 | b2;
    }
    return borrow;
  }

  static void addSmall(long[] a, long addend) {
    long carry = addend;
    for (int i = 0; i < a.length && carry != 0L; i++) {
      long sum = a[i] + carry;
      carry = Long.compareUnsigned(sum, a[i]) < 0 ? 1L : 0L;
      a[i] = sum;
    }
    if (carry != 0L) {
      throw new ArithmeticException("limb add overflow");
    }
  }

  /** In-place {@code a *= m} for {@code m} that fits in 32 bits unsigned. */
  static void multiplySmall(long[] a, long m) {
    if (m == 0L) {
      clear(a);
      return;
    }
    long carry = 0L;
    for (int i = 0; i < a.length; i++) {
      long lo = a[i] * m;
      long hi = UInt128.unsignedMultiplyHigh(a[i], m);
      long sum = lo + carry;
      long c2 = Long.compareUnsigned(sum, lo) < 0 ? 1L : 0L;
      a[i] = sum;
      carry = hi + c2;
    }
    if (carry != 0L) {
      throw new ArithmeticException("limb multiply overflow");
    }
  }

  /** Schoolbook {@code dest = a * b}. {@code dest} must not alias {@code a} or {@code b}. */
  static void multiply(long[] dest, long[] a, long[] b) {
    clear(dest);
    int na = usedLimbs(a);
    int nb = usedLimbs(b);
    if (na == 0 || nb == 0) {
      return;
    }
    for (int i = 0; i < na; i++) {
      long carry = 0L;
      for (int j = 0; j < nb; j++) {
        int k = i + j;
        if (k >= dest.length) {
          throw new ArithmeticException("limb product overflow");
        }
        long lo = a[i] * b[j];
        long hi = UInt128.unsignedMultiplyHigh(a[i], b[j]);
        long sum = dest[k] + lo;
        long c1 = Long.compareUnsigned(sum, dest[k]) < 0 ? 1L : 0L;
        long tot = sum + carry;
        long c2 = Long.compareUnsigned(tot, sum) < 0 ? 1L : 0L;
        dest[k] = tot;
        carry = hi + c1 + c2;
      }
      int k = i + nb;
      if (carry != 0L) {
        if (k >= dest.length) {
          throw new ArithmeticException("limb product overflow");
        }
        long sum = dest[k] + carry;
        long c = Long.compareUnsigned(sum, dest[k]) < 0 ? 1L : 0L;
        dest[k] = sum;
        if (c != 0L) {
          if (k + 1 >= dest.length) {
            throw new ArithmeticException("limb product overflow");
          }
          dest[k + 1] += c;
        }
      }
    }
  }

  static void multiplyPow10(long[] a, int exp, long[] scratch) {
    if (exp == 0) {
      return;
    }
    if (exp < 0 || exp > POW10_MAX) {
      throw new IllegalArgumentException("pow10 exponent out of range");
    }
    multiply(scratch, a, POW10[exp]);
    copy(a, scratch);
  }

  /**
   * Divides {@code a} by {@code 10^digits} in place. Returns the first discarded
   * decimal digit (0..9) and sets {@code sticky[0]} if any remaining discarded
   * digits are nonzero.
   */
  static int dividePow10(long[] a, int digits, boolean[] sticky) {
    sticky[0] = false;
    if (digits <= 0) {
      return 0;
    }
    int remaining = digits;
    while (remaining > 9) {
      long rem = divideSmall32(a, 1_000_000_000L);
      sticky[0] |= rem != 0L;
      remaining -= 9;
    }
    int first;
    if (remaining == 1) {
      first = (int) divideSmall32(a, 10L);
    } else {
      long pow = 1L;
      for (int i = 0; i < remaining; i++) {
        pow *= 10L;
      }
      long rem = divideSmall32(a, pow);
      long lead = pow / 10L;
      first = (int) (rem / lead);
      sticky[0] |= rem % lead != 0L;
    }
    return first;
  }

  /**
   * Unsigned division of a 64-bit-limb number by a divisor that fits in 32
   * bits. Returns the remainder.
   */
  static long divideSmall32(long[] a, long divisor) {
    if (divisor <= 0L || divisor > MASK32) {
      throw new IllegalArgumentException("divisor must fit in 32 bits");
    }
    long rem = 0L;
    for (int i = a.length - 1; i >= 0; i--) {
      long high = a[i] >>> 32;
      long low = a[i] & MASK32;
      long cur = (rem << 32) | high;
      long qh = Long.divideUnsigned(cur, divisor);
      rem = Long.remainderUnsigned(cur, divisor);
      cur = (rem << 32) | low;
      long ql = Long.divideUnsigned(cur, divisor);
      rem = Long.remainderUnsigned(cur, divisor);
      a[i] = (qh << 32) | ql;
    }
    return rem;
  }

  /**
   * Knuth Algorithm D: {@code quot = floor(u / v)}, {@code rem = u % v}.
   * {@code u} and {@code v} are little-endian unsigned. The four {@code int[]}
   * arguments are 32-bit digit scratch, each of length {@link #DIGITS32}.
   */
  static void divide(
      long[] quot,
      long[] rem,
      long[] u,
      long[] v,
      int[] ushift,
      int[] vshift,
      int[] qDigits,
      int[] remDigits) {
    clear(quot);
    clear(rem);
    if (isZero(v)) {
      throw new ArithmeticException("division by zero");
    }
    if (isZero(u) || compare(u, v) < 0) {
      copy(rem, u);
      return;
    }
    zeroDigits(ushift);
    zeroDigits(vshift);
    zeroDigits(qDigits);
    zeroDigits(remDigits);
    int m = toDigits32(u, remDigits);
    int n = toDigits32(v, qDigits);
    while (m > 1 && remDigits[m - 1] == 0) {
      m--;
    }
    while (n > 1 && qDigits[n - 1] == 0) {
      n--;
    }
    if (n == 1) {
      long d = qDigits[0] & MASK32;
      long r = 0L;
      for (int j = m - 1; j >= 0; j--) {
        long cur = (r << 32) | (remDigits[j] & MASK32);
        qDigits[j] = (int) Long.divideUnsigned(cur, d);
        r = Long.remainderUnsigned(cur, d);
      }
      fromDigits32(qDigits, m, quot);
      rem[0] = r;
      return;
    }
    System.arraycopy(remDigits, 0, ushift, 0, m);
    System.arraycopy(qDigits, 0, vshift, 0, n);
    zeroDigits(qDigits);
    knuthD(ushift, m, vshift, n, qDigits, remDigits);
    fromDigits32(qDigits, m - n + 1, quot);
    fromDigits32(remDigits, n, rem);
  }

  /** Integer square root {@code floor(sqrt(n))} and remainder {@code n - root^2}. */
  static void sqrtFloor(
      long[] root,
      long[] remainder,
      long[] n,
      long[] guess,
      long[] quot,
      long[] tmp,
      int[] d0,
      int[] d1,
      int[] d2,
      int[] d3) {
    clear(root);
    clear(remainder);
    if (isZero(n)) {
      return;
    }
    int bits = bitLength(n);
    int rootBits = (bits + 1) >>> 1;
    clear(guess);
    guess[rootBits >>> 6] = 1L << (rootBits & 63);
    while (true) {
      divide(quot, tmp, n, guess, d0, d1, d2, d3);
      add(tmp, quot, guess);
      shiftRightOne(tmp);
      if (compare(tmp, guess) >= 0) {
        copy(root, guess);
        multiply(quot, root, root);
        subtract(remainder, n, quot);
        return;
      }
      copy(guess, tmp);
    }
  }

  static void shiftRightOne(long[] a) {
    long prev = 0L;
    for (int i = a.length - 1; i >= 0; i--) {
      long next = a[i] & 1L;
      a[i] = (a[i] >>> 1) | (prev << 63);
      prev = next;
    }
  }

  static long[] pow10(int exp) {
    if (exp < 0 || exp > POW10_MAX) {
      throw new IllegalArgumentException("pow10 exponent out of range");
    }
    return POW10[exp];
  }

  static int usedLimbs(long[] a) {
    for (int i = a.length - 1; i >= 0; i--) {
      if (a[i] != 0L) {
        return i + 1;
      }
    }
    return 0;
  }

  private static int toDigits32(long[] words, int[] digits) {
    for (int i = 0; i < digits.length; i++) {
      digits[i] = 0;
    }
    int n = 0;
    for (int i = 0; i < words.length; i++) {
      digits[n++] = (int) words[i];
      digits[n++] = (int) (words[i] >>> 32);
    }
    while (n > 1 && digits[n - 1] == 0) {
      n--;
    }
    return n;
  }

  private static void fromDigits32(int[] digits, int n, long[] words) {
    clear(words);
    int lim = Math.min(n, digits.length);
    for (int i = 0; i < lim; i++) {
      int word = i >>> 1;
      if (word >= words.length) {
        if (digits[i] != 0) {
          throw new ArithmeticException("digit overflow");
        }
        continue;
      }
      long d = digits[i] & MASK32;
      if ((i & 1) == 0) {
        words[word] |= d;
      } else {
        words[word] |= d << 32;
      }
    }
  }

  /**
   * Knuth D on base-2^32 little-endian digits. {@code u} has {@code m} digits
   * and is overwritten with the shifted dividend. {@code v} has {@code n}
   * digits ({@code n >= 2}) and is overwritten with the shifted divisor.
   * {@code q} receives {@code m-n+1} quotient digits. {@code remOut} receives
   * {@code n} remainder digits.
   */
  private static void knuthD(int[] u, int m, int[] v, int n, int[] q, int[] remOut) {
    int s = Integer.numberOfLeadingZeros(v[n - 1]);
    int[] vn = remOut;
    shiftLeft32Digits(v, n, s, vn);
    System.arraycopy(vn, 0, v, 0, n);
    int[] un = u;
    int high = 0;
    if (s != 0) {
      high = (int) ((u[m - 1] & MASK32) >>> (32 - s));
      shiftLeft32Digits(u, m, s, un);
    }
    un[m] = high;
    for (int j = m - n; j >= 0; j--) {
      long ujn = un[j + n] & MASK32;
      long ujn1 = un[j + n - 1] & MASK32;
      long ujn2 = un[j + n - 2] & MASK32;
      long vn1 = v[n - 1] & MASK32;
      long vn2 = v[n - 2] & MASK32;
      long qhat;
      long rhat;
      if (ujn == vn1) {
        qhat = MASK32;
        rhat = ujn + ujn1;
      } else {
        long num = (ujn << 32) | ujn1;
        qhat = Long.divideUnsigned(num, vn1);
        rhat = Long.remainderUnsigned(num, vn1);
      }
      while (true) {
        if (rhat <= MASK32
            && Long.compareUnsigned(qhat * vn2, (rhat << 32) | ujn2) > 0) {
          qhat--;
          rhat += vn1;
          continue;
        }
        break;
      }
      long borrow = subtractMul(un, j, v, n, qhat);
      if (borrow != 0L) {
        qhat--;
        addAt(un, j, v, n);
      }
      q[j] = (int) qhat;
    }
    for (int i = m - n + 1; i < q.length; i++) {
      q[i] = 0;
    }
    shiftRight32Digits(un, n, s, remOut);
  }

  private static void zeroDigits(int[] digits) {
    for (int i = 0; i < digits.length; i++) {
      digits[i] = 0;
    }
  }

  private static void shiftLeft32Digits(int[] src, int n, int s, int[] dest) {
    if (s == 0) {
      System.arraycopy(src, 0, dest, 0, n);
      return;
    }
    int t = 32 - s;
    int carry = 0;
    for (int i = 0; i < n; i++) {
      int next = src[i];
      dest[i] = (int) (((next & MASK32) << s) | carry);
      carry = (int) ((next & MASK32) >>> t);
    }
  }

  private static void shiftRight32Digits(int[] src, int n, int s, int[] dest) {
    if (s == 0) {
      System.arraycopy(src, 0, dest, 0, n);
      for (int i = n; i < dest.length; i++) {
        dest[i] = 0;
      }
      return;
    }
    int t = 32 - s;
    int carry = 0;
    for (int i = n - 1; i >= 0; i--) {
      int next = src[i];
      dest[i] = (int) ((next & MASK32) >>> s | ((long) carry << t));
      carry = next;
    }
    for (int i = n; i < dest.length; i++) {
      dest[i] = 0;
    }
  }

  private static long subtractMul(int[] u, int j, int[] v, int n, long qhat) {
    long k = 0L;
    for (int i = 0; i < n; i++) {
      long p = qhat * (v[i] & MASK32);
      long t = (u[j + i] & MASK32) - k - (p & MASK32);
      u[j + i] = (int) t;
      k = (p >>> 32) - (t >> 32);
    }
    long t = (u[j + n] & MASK32) - k;
    u[j + n] = (int) t;
    return t >> 32;
  }

  private static void addAt(int[] u, int j, int[] v, int n) {
    long carry = 0L;
    for (int i = 0; i < n; i++) {
      long sum = (u[j + i] & MASK32) + (v[i] & MASK32) + carry;
      u[j + i] = (int) sum;
      carry = sum >>> 32;
    }
    long sum = (u[j + n] & MASK32) + carry;
    u[j + n] = (int) sum;
  }

  private static long[][] buildPow10() {
    long[][] table = new long[POW10_MAX + 1][WORK];
    table[0][0] = 1L;
    for (int i = 1; i <= POW10_MAX; i++) {
      copy(table[i], table[i - 1]);
      multiplySmall(table[i], 10L);
    }
    return table;
  }
}
