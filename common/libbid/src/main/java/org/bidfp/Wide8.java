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

  void clear() {
    l0 = 0L;
    l1 = 0L;
    l2 = 0L;
    l3 = 0L;
    l4 = 0L;
    l5 = 0L;
    l6 = 0L;
    l7 = 0L;
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
