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

import java.math.BigInteger;
import java.util.Random;

/** Knuth limb add/mul/div/sqrt against {@link BigInteger}. */
public final class LimbMathTest {
  private LimbMathTest() {
  }

  public static void main(String[] args) {
    pow10Table();
    randomArithmetic();
    sqrtCases();
    System.out.println("LimbMathTest passed");
  }

  private static void pow10Table() {
    BigInteger ten = BigInteger.TEN;
    BigInteger acc = BigInteger.ONE;
    for (int i = 0; i <= 80; i++) {
      expectEq(LimbMath.pow10(i), acc, "10^" + i);
      acc = acc.multiply(ten);
    }
  }

  private static void randomArithmetic() {
    Random rng = new Random(0x6b6e_7574L);
    long[] a = new long[LimbMath.WORK];
    long[] b = new long[LimbMath.WORK];
    long[] dest = new long[LimbMath.WORK];
    long[] quot = new long[LimbMath.WORK];
    long[] rem = new long[LimbMath.WORK];
    long[] scratch = new long[LimbMath.WORK];
    int[] d0 = new int[LimbMath.DIGITS32];
    int[] d1 = new int[LimbMath.DIGITS32];
    int[] d2 = new int[LimbMath.DIGITS32];
    int[] d3 = new int[LimbMath.DIGITS32];
    threeLimbKnown();
    wide6Multiply();
    wide8Multiply();
    wideDivide();
    wideSqrt();
    for (int n = 0; n < 200; n++) {
      int wa = 1 + rng.nextInt(3);
      int wb = 1 + rng.nextInt(3);
      fill(a, wa, rng);
      fill(b, wb, rng);
      if (LimbMath.isZero(b)) {
        b[0] = 1L;
      }
      BigInteger ba = toBig(a);
      BigInteger bb = toBig(b);

      LimbMath.add(dest, a, b);
      expectEq(dest, ba.add(bb), "add");

      if (ba.compareTo(bb) >= 0) {
        LimbMath.subtract(dest, a, b);
        expectEq(dest, ba.subtract(bb), "sub");
      }

      LimbMath.multiply(dest, a, b);
      expectEq(dest, ba.multiply(bb), "mul");

      LimbMath.copy(dest, a);
      LimbMath.multiplyPow10(dest, 9, scratch);
      expectEq(dest, ba.multiply(BigInteger.TEN.pow(9)), "mul10^9");

      LimbMath.divide(quot, rem, a, b, d0, d1, d2, d3);
      BigInteger[] qr = ba.divideAndRemainder(bb);
      expectEq(quot, qr[0], "div q");
      expectEq(rem, qr[1], "div r");
    }
  }

  private static void threeLimbKnown() {
    long[] a = new long[LimbMath.WORK];
    long[] b = new long[LimbMath.WORK];
    long[] dest = new long[LimbMath.WORK];
    long[] quot = new long[LimbMath.WORK];
    long[] rem = new long[LimbMath.WORK];
    int[] d0 = new int[LimbMath.DIGITS32];
    int[] d1 = new int[LimbMath.DIGITS32];
    int[] d2 = new int[LimbMath.DIGITS32];
    int[] d3 = new int[LimbMath.DIGITS32];
    a[0] = 0x0123_4567_89ab_cdefL;
    a[1] = 0xfedc_ba98_7654_3210L;
    a[2] = 0x0000_0000_0000_00ffL;
    b[0] = 0x1111_1111_1111_1111L;
    b[1] = 0x2222_2222_2222_2222L;
    b[2] = 0x0000_0000_0000_0003L;
    BigInteger ba = toBig(a);
    BigInteger bb = toBig(b);
    LimbMath.multiply(dest, a, b);
    expectEq(dest, ba.multiply(bb), "3-limb mul");
    LimbMath.divide(quot, rem, a, b, d0, d1, d2, d3);
    BigInteger[] qr = ba.divideAndRemainder(bb);
    expectEq(quot, qr[0], "3-limb div q");
    expectEq(rem, qr[1], "3-limb div r");
  }

  private static void wide6Multiply() {
    Wide6 a = new Wide6();
    Wide6 b = new Wide6();
    Wide6 r = new Wide6();
    a.l0 = 0x0123_4567_89ab_cdefL;
    a.l1 = 0xfedc_ba98_7654_3210L;
    a.l2 = 0x0000_0000_0000_00ffL;
    b.l0 = 0x1111_1111_1111_1111L;
    b.l1 = 0x2222_2222_2222_2222L;
    b.l2 = 0x0000_0000_0000_0003L;
    r.multiply3(a, b);
    long[] dest = new long[LimbMath.WORK];
    r.store(dest);
    expectEq(dest, toBig(new long[] {a.l0, a.l1, a.l2}).multiply(
        toBig(new long[] {b.l0, b.l1, b.l2})), "wide6 mul3");
  }

  private static void wide8Multiply() {
    Wide8 a = new Wide8();
    Wide8 b = new Wide8();
    Wide8 r = new Wide8();
    a.l0 = 0x0123_4567_89ab_cdefL;
    a.l1 = 0xfedc_ba98_7654_3210L;
    a.l2 = 0x0000_0000_0000_00ffL;
    a.l3 = 0x123L;
    b.l0 = 0x1111_1111_1111_1111L;
    b.l1 = 0x2222_2222_2222_2222L;
    b.l2 = 0x0000_0000_0000_0003L;
    b.l3 = 0x456L;
    r.multiply4(a, b);
    long[] dest = new long[LimbMath.WORK];
    r.store(dest);
    expectEq(dest, toBig(new long[] {a.l0, a.l1, a.l2, a.l3}).multiply(
        toBig(new long[] {b.l0, b.l1, b.l2, b.l3})), "wide8 mul4");
  }

  private static void wideDivide() {
    Random rng = new Random(0x6469_7636L);
    Wide6 ua = new Wide6();
    Wide6 va = new Wide6();
    Wide6 qa = new Wide6();
    Wide6 na = new Wide6();
    Wide8 ub = new Wide8();
    Wide8 vb = new Wide8();
    Wide8 qb = new Wide8();
    Wide8 nb = new Wide8();
    long[] buf = new long[LimbMath.WORK];
    for (int n = 0; n < 400; n++) {
      fillWide6(ua, 1 + rng.nextInt(6), rng);
      fillWide6(va, 1 + rng.nextInt(3), rng);
      if (va.isZero()) {
        va.l0 = 1L;
      }
      BigInteger bu = toBig(store(ua, buf));
      BigInteger bv = toBig(store(va, buf));
      qa.divide(ua, va, na);
      BigInteger[] qr = bu.divideAndRemainder(bv);
      expectEq(store(qa, buf), qr[0], "wide6 div q");
      expectEq(store(ua, buf), qr[1], "wide6 div r");

      fillWide8(ub, 1 + rng.nextInt(8), rng);
      fillWide8(vb, 1 + rng.nextInt(4), rng);
      if (vb.isZero()) {
        vb.l0 = 1L;
      }
      bu = toBig(store(ub, buf));
      bv = toBig(store(vb, buf));
      qb.divide(ub, vb, nb);
      qr = bu.divideAndRemainder(bv);
      expectEq(store(qb, buf), qr[0], "wide8 div q");
      expectEq(store(ub, buf), qr[1], "wide8 div r");
    }
  }

  private static void fillWide6(Wide6 dest, int words, Random rng) {
    dest.clear();
    dest.l0 = rng.nextLong();
    if (words > 1) {
      dest.l1 = rng.nextLong();
    }
    if (words > 2) {
      dest.l2 = rng.nextLong();
    }
    if (words > 3) {
      dest.l3 = rng.nextLong();
    }
    if (words > 4) {
      dest.l4 = rng.nextLong();
    }
    if (words > 5) {
      dest.l5 = rng.nextLong();
    }
  }

  private static void fillWide8(Wide8 dest, int words, Random rng) {
    dest.clear();
    dest.l0 = rng.nextLong();
    if (words > 1) {
      dest.l1 = rng.nextLong();
    }
    if (words > 2) {
      dest.l2 = rng.nextLong();
    }
    if (words > 3) {
      dest.l3 = rng.nextLong();
    }
    if (words > 4) {
      dest.l4 = rng.nextLong();
    }
    if (words > 5) {
      dest.l5 = rng.nextLong();
    }
    if (words > 6) {
      dest.l6 = rng.nextLong();
    }
    if (words > 7) {
      dest.l7 = rng.nextLong();
    }
  }

  private static long[] store(Wide6 src, long[] buf) {
    src.store(buf);
    return buf;
  }

  private static long[] store(Wide8 src, long[] buf) {
    src.store(buf);
    return buf;
  }

  private static void wideSqrt() {
    Random rng = new Random(0x7371_7274L);
    long[] buf = new long[LimbMath.WORK];
    Wide6 n6 = new Wide6();
    Wide6 g6 = new Wide6();
    Wide6 q6 = new Wide6();
    Wide6 t6 = new Wide6();
    Wide6 v6 = new Wide6();
    Wide8 n8 = new Wide8();
    Wide8 g8 = new Wide8();
    Wide8 q8 = new Wide8();
    Wide8 t8 = new Wide8();
    Wide8 v8 = new Wide8();
    Wide12 n12 = new Wide12();
    Wide12 g12 = new Wide12();
    Wide12 q12 = new Wide12();
    Wide12 t12 = new Wide12();
    Wide12 v12 = new Wide12();
    long[] known = {0L, 1L, 3L, 81L, 10_000L, Long.MAX_VALUE};
    for (long v : known) {
      n6.clear();
      n6.l0 = v;
      Wide6.sqrtFloor(n6, g6, q6, t6, v6);
      n8.clear();
      n8.l0 = v;
      Wide8.sqrtFloor(n8, g8, q8, t8, v8);
      BigInteger bn = unsigned(v);
      BigInteger br = bn.sqrt();
      expectEq(store(g6, buf), br, "wide6 sqrt " + v);
      expectEq(store(g8, buf), br, "wide8 sqrt " + v);
    }
    for (int n = 0; n < 80; n++) {
      fillWide6(n6, 1 + rng.nextInt(5), rng);
      BigInteger bn6 = toBig(store(n6, buf));
      boolean sticky6 = Wide6.sqrtFloor(n6, g6, q6, t6, v6);
      BigInteger br6 = bn6.sqrt();
      expectEq(store(g6, buf), br6, "wide6 sqrt q");
      BigInteger rem6 = bn6.subtract(br6.multiply(br6));
      if (sticky6 != (rem6.signum() != 0)) {
        throw new IllegalStateException("wide6 sqrt sticky");
      }
      fillWide8(n8, 1 + rng.nextInt(6), rng);
      BigInteger bn = toBig(store(n8, buf));
      boolean sticky = Wide8.sqrtFloor(n8, g8, q8, t8, v8);
      BigInteger br = bn.sqrt();
      expectEq(store(g8, buf), br, "wide8 sqrt q");
      BigInteger rem = bn.subtract(br.multiply(br));
      if (sticky != (rem.signum() != 0)) {
        throw new IllegalStateException("wide8 sqrt sticky");
      }
      fillWide12(n12, 1 + rng.nextInt(10), rng);
      bn = toBig(store(n12, buf));
      sticky = Wide12.sqrtFloor(n12, g12, q12, t12, v12);
      br = bn.sqrt();
      expectEq(store(n12, buf), bn, "wide12 n kept");
      expectEq(store(g12, buf), br, "wide12 sqrt q");
      rem = bn.subtract(br.multiply(br));
      if (sticky != (rem.signum() != 0)) {
        throw new IllegalStateException("wide12 sqrt sticky");
      }
    }
  }

  private static void fillWide12(Wide12 dest, int words, Random rng) {
    dest.clear();
    for (int i = 0; i < words; i++) {
      dest.setLimb(i, rng.nextLong());
    }
  }

  private static long[] store(Wide12 src, long[] buf) {
    src.store(buf);
    return buf;
  }

  private static void sqrtCases() {
    long[] n = new long[LimbMath.WORK];
    long[] root = new long[LimbMath.WORK];
    long[] rem = new long[LimbMath.WORK];
    long[] guess = new long[LimbMath.WORK];
    long[] quot = new long[LimbMath.WORK];
    long[] tmp = new long[LimbMath.WORK];
    int[] d0 = new int[LimbMath.DIGITS32];
    int[] d1 = new int[LimbMath.DIGITS32];
    int[] d2 = new int[LimbMath.DIGITS32];
    int[] d3 = new int[LimbMath.DIGITS32];
    long[] values = {0L, 1L, 3L, 81L, 10_000L, Long.MAX_VALUE};
    for (long v : values) {
      LimbMath.clear(n);
      n[0] = v;
      LimbMath.sqrtFloor(root, rem, n, guess, quot, tmp, d0, d1, d2, d3);
      BigInteger bn = BigInteger.valueOf(v);
      BigInteger br = bn.sqrt();
      expectEq(root, br, "sqrt " + v);
      expectEq(rem, bn.subtract(br.multiply(br)), "sqrt rem " + v);
    }
  }

  private static void fill(long[] dest, int words, Random rng) {
    LimbMath.clear(dest);
    for (int i = 0; i < words; i++) {
      dest[i] = rng.nextLong();
    }
  }

  private static BigInteger toBig(long[] limbs) {
    BigInteger value = BigInteger.ZERO;
    for (int i = limbs.length - 1; i >= 0; i--) {
      value = value.shiftLeft(64).add(unsigned(limbs[i]));
    }
    return value;
  }

  private static BigInteger unsigned(long word) {
    if (word >= 0L) {
      return BigInteger.valueOf(word);
    }
    return BigInteger.valueOf(word).add(BigInteger.ONE.shiftLeft(64));
  }

  private static void expectEq(long[] limbs, BigInteger expected, String label) {
    BigInteger got = toBig(limbs);
    if (!got.equals(expected)) {
      throw new IllegalStateException(label + " got " + got + " expected " + expected);
    }
  }
}
