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

import java.util.Arrays;

/**
 * IEEE 754 decimal interchange value of {@code k} 32-bit storage units
 * (packed width {@code 32*k} bits).
 *
 * <p>{@code k=2} and {@code k=4} dispatch to {@link Bid64} and {@link Bid128}.
 * {@code k=3} is a Decimal96 specialist. {@code k=5} uses Wide6, and
 * {@code k=6..8} use Wide8. Repeated arithmetic should use
 * {@link BidUnpacked} and pack only at I/O.
 *
 * <p>Transcendental functions are provided only for {@code k<=4} (packed
 * width 128) by converting through {@link Bid128} and the binary128 DPML
 * engine. Wider formats throw {@link UnsupportedOperationException} until a
 * binary256 engine is funded.
 */
public final class BidK {
  private final FormatParams format;
  private final long[] words;

  private BidK(FormatParams format, long[] words) {
    this.format = format;
    this.words = words;
  }

  public static BidK fromRawBits(FormatParams format, long[] words) {
    if (words.length < format.wordCount()) {
      throw new IllegalArgumentException("packed encoding is shorter than 32*k bits");
    }
    return new BidK(format, BidCodec.cloneWords(format, words));
  }

  public static BidK parse(
      FormatParams format, String text, RoundingMode mode, StatusFlags flags) {
    long[] words = new long[format.wordCount()];
    BidKRaw.parse(format, text, mode, flags, words);
    return new BidK(format, words);
  }

  public static BidK parse(FormatParams format, String text) {
    return parse(format, text, RoundingMode.TIES_TO_EVEN, new StatusFlags());
  }

  public static BidK infinity(FormatParams format, boolean negative) {
    long[] words = new long[format.wordCount()];
    BidCodec.packInfinity(format, negative, words);
    return new BidK(format, words);
  }

  public static BidK naN(FormatParams format, boolean negative, boolean signaling) {
    long[] words = new long[format.wordCount()];
    BidCodec.packNaN(format, negative, signaling, words);
    return new BidK(format, words);
  }

  public static BidK fromBid64(
      Bid64 value, FormatParams dest, RoundingMode mode, StatusFlags flags) {
    long[] src = {value.toRawBits()};
    long[] out = new long[dest.wordCount()];
    BidKRaw.convert(FormatParams.DECIMAL64, src, dest, mode, flags, out);
    return new BidK(dest, out);
  }

  public static BidK fromBid128(
      Bid128 value, FormatParams dest, RoundingMode mode, StatusFlags flags) {
    long[] src = {value.lowBits(), value.highBits()};
    long[] out = new long[dest.wordCount()];
    BidKRaw.convert(FormatParams.DECIMAL128, src, dest, mode, flags, out);
    return new BidK(dest, out);
  }

  public FormatParams format() {
    return format;
  }

  public long[] toRawBits() {
    return BidCodec.cloneWords(format, words);
  }

  public boolean isSigned() {
    return BidCodec.isSigned(format, words);
  }

  public boolean isFinite() {
    return BidCodec.isFinite(format, words);
  }

  public boolean isInfinite() {
    return BidCodec.isInfinite(format, words);
  }

  public boolean isNaN() {
    return BidCodec.isNaN(format, words);
  }

  public boolean isSignalingNaN() {
    return BidCodec.isSignalingNaN(format, words);
  }

  public BidK add(BidK other, RoundingMode mode, StatusFlags flags) {
    checkSameFormat(other);
    long[] out = new long[format.wordCount()];
    BidKRaw.add(format, words, other.words, mode, flags, out);
    return new BidK(format, out);
  }

  public BidK subtract(BidK other, RoundingMode mode, StatusFlags flags) {
    checkSameFormat(other);
    long[] out = new long[format.wordCount()];
    BidKRaw.sub(format, words, other.words, mode, flags, out);
    return new BidK(format, out);
  }

  public BidK multiply(BidK other, RoundingMode mode, StatusFlags flags) {
    checkSameFormat(other);
    long[] out = new long[format.wordCount()];
    BidKRaw.mul(format, words, other.words, mode, flags, out);
    return new BidK(format, out);
  }

  public BidK divide(BidK other, RoundingMode mode, StatusFlags flags) {
    checkSameFormat(other);
    long[] out = new long[format.wordCount()];
    BidKRaw.div(format, words, other.words, mode, flags, out);
    return new BidK(format, out);
  }

  public BidK sqrt(RoundingMode mode, StatusFlags flags) {
    long[] out = new long[format.wordCount()];
    BidKRaw.sqrt(format, words, mode, flags, out);
    return new BidK(format, out);
  }

  public BidK fma(BidK y, BidK z, RoundingMode mode, StatusFlags flags) {
    checkSameFormat(y);
    checkSameFormat(z);
    long[] out = new long[format.wordCount()];
    BidKRaw.fma(format, words, y.words, z.words, mode, flags, out);
    return new BidK(format, out);
  }

  public BidK quantize(BidK y, RoundingMode mode, StatusFlags flags) {
    checkSameFormat(y);
    long[] out = new long[format.wordCount()];
    BidKRaw.quantize(format, words, y.words, mode, flags, out);
    return new BidK(format, out);
  }

  public BidK convert(FormatParams dest, RoundingMode mode, StatusFlags flags) {
    long[] out = new long[dest.wordCount()];
    BidKRaw.convert(format, words, dest, mode, flags, out);
    return new BidK(dest, out);
  }

  public Bid64 toBid64(RoundingMode mode, StatusFlags flags) {
    BidK converted = convert(FormatParams.DECIMAL64, mode, flags);
    return Bid64.fromRawBits(converted.words[0]);
  }

  public Bid128 toBid128(RoundingMode mode, StatusFlags flags) {
    BidK converted = convert(FormatParams.DECIMAL128, mode, flags);
    long high = converted.words.length > 1 ? converted.words[1] : 0L;
    return Bid128.fromRawBits(high, converted.words[0]);
  }

  public BidK exp(RoundingMode mode, StatusFlags flags) {
    BidKRaw.requireLibm(format);
    return fromBid128(toBid128(mode, flags).exp(mode, flags), format, mode, flags);
  }

  public BidK log(RoundingMode mode, StatusFlags flags) {
    BidKRaw.requireLibm(format);
    return fromBid128(toBid128(mode, flags).log(mode, flags), format, mode, flags);
  }

  public BidK sin(RoundingMode mode, StatusFlags flags) {
    BidKRaw.requireLibm(format);
    return fromBid128(toBid128(mode, flags).sin(mode, flags), format, mode, flags);
  }

  /** Spark SQL ordering: signed zeros equal, all NaNs equal and greatest. */
  public int sqlCompare(BidK other) {
    checkSameFormat(other);
    return BidKRaw.sqlCompare(format, words, other.words);
  }

  public boolean sqlEquals(BidK other) {
    return sqlCompare(other) == 0;
  }

  @Override
  public String toString() {
    return BidKRaw.toString(format, words);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof BidK)) {
      return false;
    }
    BidK that = (BidK) other;
    return format.packedBits() == that.format.packedBits() && Arrays.equals(words, that.words);
  }

  @Override
  public int hashCode() {
    return 31 * format.packedBits() + Arrays.hashCode(words);
  }

  private void checkSameFormat(BidK other) {
    if (format.packedBits() != other.format.packedBits()) {
      throw new IllegalArgumentException("BID-k operands must use the same format");
    }
  }
}
