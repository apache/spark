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
