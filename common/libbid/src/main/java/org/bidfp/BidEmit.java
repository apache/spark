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
 * Writes a kernel result as packed IEEE words, or into
 * {@link BidKScratch.Frame#unpackedOut} when that is set.
 */
final class BidEmit {
  private BidEmit() {
  }

  static void finite(
      FormatParams format,
      boolean negative,
      int biasedExponent,
      Wide6 coefficient,
      long[] words,
      BidKScratch.Frame frame) {
    if (emitFinite(
        format, negative, biasedExponent, frame,
        coefficient.l0, coefficient.l1, coefficient.l2, coefficient.l3)) {
      return;
    }
    BidCodec.packFinite(format, negative, biasedExponent, coefficient, words);
  }

  static void finite(
      FormatParams format,
      boolean negative,
      int biasedExponent,
      Wide8 coefficient,
      long[] words,
      BidKScratch.Frame frame) {
    if (emitFinite(
        format, negative, biasedExponent, frame,
        coefficient.l0, coefficient.l1, coefficient.l2, coefficient.l3)) {
      return;
    }
    BidCodec.packFinite(format, negative, biasedExponent, coefficient, words);
  }

  static void finite(
      FormatParams format,
      boolean negative,
      int biasedExponent,
      UIntLimbs coefficient,
      long[] words,
      BidKScratch.Frame frame) {
    finite(format, negative, biasedExponent, coefficient.limbs(), words, frame);
  }

  static void finite(
      FormatParams format,
      boolean negative,
      int biasedExponent,
      long[] coefficient,
      long[] words,
      BidKScratch.Frame frame) {
    long c0 = coefficient.length > 0 ? coefficient[0] : 0L;
    long c1 = coefficient.length > 1 ? coefficient[1] : 0L;
    long c2 = coefficient.length > 2 ? coefficient[2] : 0L;
    long c3 = coefficient.length > 3 ? coefficient[3] : 0L;
    if (emitFinite(format, negative, biasedExponent, frame, c0, c1, c2, c3)) {
      return;
    }
    BidCodec.packFinite(format, negative, biasedExponent, coefficient, words);
  }

  static void infinity(
      FormatParams format,
      boolean negative,
      long[] words,
      BidKScratch.Frame frame) {
    if (frame != null && frame.unpackedOut != null) {
      frame.unpackedOut.setInfinity(format, negative);
      return;
    }
    BidCodec.packInfinity(format, negative, words);
  }

  static void nan(
      FormatParams format,
      boolean negative,
      boolean signaling,
      long[] words,
      BidKScratch.Frame frame) {
    if (frame != null && frame.unpackedOut != null) {
      frame.unpackedOut.setNaN(format, negative, signaling);
      return;
    }
    BidCodec.packNaN(format, negative, signaling, words);
  }

  private static boolean emitFinite(
      FormatParams format,
      boolean negative,
      int biasedExponent,
      BidKScratch.Frame frame,
      long c0,
      long c1,
      long c2,
      long c3) {
    if (frame == null || frame.unpackedOut == null) {
      return false;
    }
    frame.unpackedOut.setFinite(
        format, negative, biasedExponent - format.bias(), c0, c1, c2, c3);
    return true;
  }
}
