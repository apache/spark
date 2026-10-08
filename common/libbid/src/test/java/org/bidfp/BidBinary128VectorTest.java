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

import java.io.IOException;

/** Intel {@code readtest.in} exact-bit coverage for BID &lt;-&gt; binary128. */
public final class BidBinary128VectorTest {
  private BidBinary128VectorTest() {
  }

  public static void main(String[] args) throws IOException {
    int bid64To = checkBid64ToBinary128();
    int binaryTo64 = checkBinary128ToBid64();
    int bid128To = checkBid128ToBinary128();
    int binaryTo128 = checkBinary128ToBid128();
    if (bid64To != 1516 || binaryTo64 != 1819 || bid128To != 1767
        || binaryTo128 != 1562) {
      throw new IllegalStateException(String.format(
          "unexpected convert counts: %d %d %d %d",
          bid64To, binaryTo64, bid128To, binaryTo128));
    }
    System.out.println("BidBinary128VectorTest: all tests passed ("
        + (bid64To + binaryTo64 + bid128To + binaryTo128) + " vectors)");
  }

  private static int checkBid64ToBinary128() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid64_to_binary128")) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long input = parse64(tokens[2]);
      long[] expected = IntelVectors.hex128(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      long[] actual = new long[2];
      StatusFlags flags = new StatusFlags();
      Bid64Raw.toBinary128(input, mode, flags, actual);
      if (actual[0] != expected[0] || actual[1] != expected[1]
          || flags.bits() != expectedFlags) {
        throw new IllegalStateException(String.format(
            "bid64_to_binary128 %s actual [0x%016x%016x] %02x",
            line, actual[0], actual[1], flags.bits()));
      }
      tested++;
    }
    return tested;
  }

  private static int checkBinary128ToBid64() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("binary128_to_bid64")) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] input = IntelVectors.hex128(tokens[2]);
      long expected = IntelVectors.hex64(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      if (line.contains("underflow_before_only")) {
        expectedFlags &= ~StatusFlags.UNDERFLOW;
      }
      StatusFlags flags = new StatusFlags();
      long actual = Bid64Raw.fromBinary128(input[0], input[1], mode, flags);
      if (actual != expected || flags.bits() != expectedFlags) {
        throw new IllegalStateException(String.format(
            "binary128_to_bid64 %s actual [0x%016x] %02x",
            line, actual, flags.bits()));
      }
      tested++;
    }
    return tested;
  }

  private static int checkBid128ToBinary128() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("bid128_to_binary128")) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] input = parse128(tokens[2]);
      long[] expected = IntelVectors.hex128(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      long[] actual = new long[2];
      StatusFlags flags = new StatusFlags();
      Bid128Raw.toBinary128(input[0], input[1], mode, flags, actual);
      if (actual[0] != expected[0] || actual[1] != expected[1]
          || flags.bits() != expectedFlags) {
        throw new IllegalStateException(String.format(
            "bid128_to_binary128 %s actual [0x%016x%016x] %02x",
            line, actual[0], actual[1], flags.bits()));
      }
      tested++;
    }
    return tested;
  }

  private static int checkBinary128ToBid128() throws IOException {
    int tested = 0;
    for (String line : IntelVectors.lines("binary128_to_bid128")) {
      String[] tokens = IntelVectors.tokens(line);
      if (tokens.length < 5) {
        continue;
      }
      RoundingMode mode = IntelVectors.mode(tokens[1]);
      long[] input = IntelVectors.hex128(tokens[2]);
      long[] expected = parse128(tokens[3]);
      int expectedFlags = IntelVectors.flags(tokens[4]);
      long[] actual = new long[2];
      StatusFlags flags = new StatusFlags();
      Bid128Raw.fromBinary128(input[0], input[1], mode, flags, actual);
      if (actual[0] != expected[0] || actual[1] != expected[1]
          || flags.bits() != expectedFlags) {
        throw new IllegalStateException(String.format(
            "binary128_to_bid128 %s actual [0x%016x%016x] %02x",
            line, actual[0], actual[1], flags.bits()));
      }
      tested++;
    }
    return tested;
  }

  private static long parse64(String token) {
    if (IntelVectors.isHexPayload(token) && token.contains("[")) {
      return IntelVectors.hex64(token);
    }
    return Bid64Raw.fromString(token, RoundingMode.TIES_TO_EVEN, new StatusFlags());
  }

  private static long[] parse128(String token) {
    if (IntelVectors.isHexPayload(token)) {
      return IntelVectors.hex128(token);
    }
    Bid128 value = Bid128.parseExact(token);
    return new long[] {value.highBits(), value.lowBits()};
  }
}
