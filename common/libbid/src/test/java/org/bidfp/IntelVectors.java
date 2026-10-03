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
package org.bidfp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Parses Intel RDFP {@code readtest.in} operation lines. */
final class IntelVectors {
  private static Map<String, List<String>> linesByOperation;

  private IntelVectors() {
  }

  static synchronized List<String> lines(String operation) throws IOException {
    if (linesByOperation == null) {
      linesByOperation = new HashMap<>();
      InputStream in = IntelVectors.class.getResourceAsStream("readtest.in");
      if (in == null) {
        throw new IOException("missing resource readtest.in");
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(in, StandardCharsets.US_ASCII))) {
        for (String line; (line = reader.readLine()) != null; ) {
          int separator = line.indexOf(' ');
          if (separator > 0) {
            String name = line.substring(0, separator);
            linesByOperation.computeIfAbsent(name, ignored -> new ArrayList<>()).add(line);
          }
        }
      }
    }
    return linesByOperation.getOrDefault(operation, List.of());
  }

  static RoundingMode mode(String token) {
    return RoundingMode.fromIntel(Integer.parseInt(token));
  }

  static int flags(String token) {
    String value = token.startsWith("0x") || token.startsWith("0X")
        ? token.substring(2)
        : token;
    int end = 0;
    while (end < value.length() && isHex(value.charAt(end))) {
      end++;
    }
    return Integer.parseInt(value.substring(0, Math.max(end, 1)), 16);
  }

  /**
   * Status bits from a vector line. Drops UNDERFLOW when Intel annotated
   * {@code underflow_before_only}: this port detects tininess after rounding.
   */
  static int flags(String token, String line) {
    int bits = flags(token);
    if (line.contains("underflow_before_only")) {
      return bits & ~StatusFlags.UNDERFLOW;
    }
    return bits;
  }

  static long hex64(String token) {
    String value = stripBrackets(token);
    return Long.parseUnsignedLong(value, 16);
  }

  static long[] hex128(String token) {
    String value = stripBrackets(token);
    if (value.contains(",")) {
      String[] parts = value.split(",");
      return new long[] {
        Long.parseUnsignedLong(parts[0], 16),
        Long.parseUnsignedLong(parts[1], 16)
      };
    }
    if (value.length() == 32) {
      return new long[] {
        Long.parseUnsignedLong(value.substring(0, 16), 16),
        Long.parseUnsignedLong(value.substring(16), 16)
      };
    }
    throw new IllegalArgumentException(token);
  }

  static boolean isHexPayload(String token) {
    if (!token.startsWith("[")) {
      return false;
    }
    String value = stripBrackets(token);
    if (value.contains(",")) {
      return true;
    }
    return value.length() == 16 || value.length() == 32;
  }

  /**
   * Parse a BID64 operand or expected encoding. Hex payloads are used as-is.
   * Decimal text uses {@link Bid64Raw#fromString} with ties-to-even, matching
   * Intel {@code readtest.c} operand conversion (the line rounding applies to
   * the operation, not to reading decimal text).
   */
  static long operand64(String token) {
    if (isHexPayload(token)) {
      return token.contains(",") ? hex128(token)[1] : hex64(token);
    }
    if (token.equalsIgnoreCase("QNaN")) {
      return Bid64.QUIET_NAN.toRawBits();
    }
    if (isSpecial(token)) {
      return Bid64.parseExact(token).toRawBits();
    }
    return Bid64Raw.fromString(token, RoundingMode.TIES_TO_EVEN, new StatusFlags());
  }

  /**
   * Parse a BID128 operand or expected encoding. Accepts comma-separated and
   * 32-hex no-comma payloads.
   */
  static long[] operand128(String token) {
    if (isHexPayload(token)) {
      return hex128(token);
    }
    if (token.equalsIgnoreCase("QNaN")) {
      return new long[] {Bid128.QUIET_NAN.highBits(), Bid128.QUIET_NAN.lowBits()};
    }
    if (isSpecial(token)) {
      Bid128 value = Bid128.parseExact(token);
      return new long[] {value.highBits(), value.lowBits()};
    }
    long[] result = new long[2];
    Bid128Raw.fromString(
        token, RoundingMode.TIES_TO_EVEN, new StatusFlags(), result);
    return result;
  }

  private static boolean isSpecial(String token) {
    String upper = token.toUpperCase(Locale.ROOT);
    return upper.endsWith("NAN") || upper.endsWith("INF") || upper.endsWith("INFINITY");
  }

  private static String stripBrackets(String token) {
    String value = token;
    if (value.startsWith("[")) {
      value = value.substring(1);
    }
    if (value.endsWith("]")) {
      value = value.substring(0, value.length() - 1);
    }
    return value;
  }

  private static boolean isHex(char c) {
    char u = Character.toUpperCase(c);
    return u >= '0' && u <= '9' || u >= 'A' && u <= 'F';
  }

  static String[] tokens(String line) {
    String withoutUlp = line;
    int ulp = line.toLowerCase(Locale.ROOT).indexOf(" ulp=");
    if (ulp >= 0) {
      withoutUlp = line.substring(0, ulp);
    }
    return withoutUlp.trim().split("\\s+");
  }

  static double ulp(String line) {
    String lower = line.toLowerCase(Locale.ROOT);
    int start = lower.indexOf(" ulp=");
    if (start < 0) {
      return 0.0;
    }
    start += 5;
    int end = start;
    while (end < line.length() && !Character.isWhitespace(line.charAt(end))) {
      end++;
    }
    return Double.parseDouble(line.substring(start, end));
  }
}
