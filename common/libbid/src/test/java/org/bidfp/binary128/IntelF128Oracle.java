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
package org.bidfp.binary128;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Loads packed binary128 vectors produced by Intel DPML {@code bid_f128_*}
 * on x86_64 (soft quad, {@code USE_COMPILER_F128_TYPE=0}).
 *
 * <p>Format: {@code op rnd xhi xlo [yhi ylo] rhi rlo} with 16 hex digits.
 * {@code rnd} is always 0: the C entry points do not take a rounding mode.
 */
final class IntelF128Oracle {
  private IntelF128Oracle() {
  }

  static List<Case> load() throws IOException {
    String name = "intel-f128-oracle.txt";
    InputStream in = IntelF128Oracle.class.getResourceAsStream(name);
    if (in == null) {
      throw new IOException("missing resource " + name);
    }
    List<Case> cases = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(in, StandardCharsets.US_ASCII))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty() || line.charAt(0) == '#') {
          continue;
        }
        cases.add(parse(line));
      }
    }
    return cases;
  }

  static Case parse(String line) {
    String[] t = line.trim().split("\\s+");
    if (t.length != 6 && t.length != 8) {
      throw new IllegalArgumentException(line);
    }
    Case c = new Case();
    c.op = t[0].toLowerCase(Locale.ROOT);
    c.rnd = Integer.parseInt(t[1]);
    c.x = bits(t[2], t[3]);
    if (t.length == 8) {
      c.y = bits(t[4], t[5]);
      c.expected = bits(t[6], t[7]);
    } else {
      c.expected = bits(t[4], t[5]);
    }
    return c;
  }

  static boolean sameBitsOrBothNaN(Binary128 a, Binary128 b) {
    return a.equals(b) || (a.isNaN() && b.isNaN());
  }

  private static final BigInteger MASK128 =
      BigInteger.ONE.shiftLeft(128).subtract(BigInteger.ONE);

  /** IEEE encoding distance; 0 if equal or both NaN. Inf mismatch is max. */
  static int ulpDistance(Binary128 a, Binary128 b) {
    if (sameBitsOrBothNaN(a, b)) {
      return 0;
    }
    if (a.isNaN() || b.isNaN() || a.isInfinite() || b.isInfinite()) {
      return Integer.MAX_VALUE;
    }
    BigInteger d = orderedBits(a).subtract(orderedBits(b)).abs();
    if (d.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
      return Integer.MAX_VALUE;
    }
    return d.intValue();
  }

  private static BigInteger orderedBits(Binary128 x) {
    BigInteger hi = unsigned(x.highBits());
    BigInteger lo = unsigned(x.lowBits());
    BigInteger bits = hi.shiftLeft(64).or(lo);
    if (x.isSigned()) {
      return bits.xor(MASK128);
    }
    return bits;
  }

  private static BigInteger unsigned(long v) {
    return new BigInteger(Long.toUnsignedString(v));
  }

  private static Binary128 bits(String hi, String lo) {
    return Binary128.fromRawBits(
        Long.parseUnsignedLong(hi, 16), Long.parseUnsignedLong(lo, 16));
  }

  static final class Case {
    String op;
    int rnd;
    Binary128 x;
    Binary128 y;
    Binary128 expected;

    boolean binary() {
      return y != null;
    }
  }
}
