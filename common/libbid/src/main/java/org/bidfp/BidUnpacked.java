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
 * Compute form of a BID-k value (IEEE {@code k} 32-bit units; packed width
 * {@code 32*k}): kind, sign, unbiased exponent, and up to four coefficient
 * limbs. Packed IEEE words stay the interchange encoding; this type is the
 * Spark execution shape.
 *
 * <p>Instances are mutable and meant to be reused. {@code out} may alias
 * {@code this} on arithmetic methods.
 */
public final class BidUnpacked {
  public enum Kind {
    FINITE,
    INF,
    NAN,
    SNAN
  }

  private FormatParams format;
  private Kind kind = Kind.FINITE;
  private boolean negative;
  private int exponent;
  private long c0;
  private long c1;
  private long c2;
  private long c3;

  public BidUnpacked(FormatParams format) {
    this.format = format;
  }

  public FormatParams format() {
    return format;
  }

  public Kind kind() {
    return kind;
  }

  public boolean negative() {
    return negative;
  }

  public int exponent() {
    return exponent;
  }

  public long c0() {
    return c0;
  }

  public long c1() {
    return c1;
  }

  public long c2() {
    return c2;
  }

  public long c3() {
    return c3;
  }

  public boolean isFinite() {
    return kind == Kind.FINITE;
  }

  public boolean isInfinite() {
    return kind == Kind.INF;
  }

  public boolean isNaN() {
    return kind == Kind.NAN || kind == Kind.SNAN;
  }

  public void copyFrom(BidUnpacked other) {
    format = other.format;
    kind = other.kind;
    negative = other.negative;
    exponent = other.exponent;
    c0 = other.c0;
    c1 = other.c1;
    c2 = other.c2;
    c3 = other.c3;
  }

  public void loadPacked(long[] words) {
    BidCodec.loadUnpacked(format, words, this);
  }

  public void storePacked(long[] words) {
    BidCodec.storeUnpacked(this, words);
  }

  public void add(
      BidUnpacked y, RoundingMode mode, StatusFlags flags, BidUnpacked out) {
    BidKRaw.addUnpacked(this, y, false, mode, flags, out);
  }

  public void subtract(
      BidUnpacked y, RoundingMode mode, StatusFlags flags, BidUnpacked out) {
    BidKRaw.addUnpacked(this, y, true, mode, flags, out);
  }

  public void multiply(
      BidUnpacked y, RoundingMode mode, StatusFlags flags, BidUnpacked out) {
    BidKRaw.mulUnpacked(this, y, mode, flags, out);
  }

  public void divide(
      BidUnpacked y, RoundingMode mode, StatusFlags flags, BidUnpacked out) {
    BidKRaw.divUnpacked(this, y, mode, flags, out);
  }

  public void sqrt(RoundingMode mode, StatusFlags flags, BidUnpacked out) {
    BidKRaw.sqrtUnpacked(this, mode, flags, out);
  }

  public void fma(
      BidUnpacked y,
      BidUnpacked z,
      RoundingMode mode,
      StatusFlags flags,
      BidUnpacked out) {
    BidKRaw.fmaUnpacked(this, y, z, mode, flags, out);
  }

  public int sqlCompare(BidUnpacked other) {
    return BidKRaw.sqlCompareUnpacked(this, other);
  }

  public boolean sqlEquals(BidUnpacked other) {
    return sqlCompare(other) == 0;
  }

  /** Hash consistent with {@link #sqlEquals(BidUnpacked)}. */
  public int sqlHash() {
    long[] words = new long[format.wordCount()];
    storePacked(words);
    return BidKRaw.sqlHash(format, words);
  }

  void setFinite(
      FormatParams format,
      boolean negative,
      int exponent,
      long c0,
      long c1,
      long c2,
      long c3) {
    this.format = format;
    this.kind = Kind.FINITE;
    this.negative = negative;
    this.exponent = exponent;
    this.c0 = c0;
    this.c1 = c1;
    this.c2 = c2;
    this.c3 = c3;
  }

  void setInfinity(FormatParams format, boolean negative) {
    this.format = format;
    this.kind = Kind.INF;
    this.negative = negative;
    this.exponent = 0;
    this.c0 = 0L;
    this.c1 = 0L;
    this.c2 = 0L;
    this.c3 = 0L;
  }

  void setNaN(FormatParams format, boolean negative, boolean signaling) {
    this.format = format;
    this.kind = signaling ? Kind.SNAN : Kind.NAN;
    this.negative = negative;
    this.exponent = 0;
    this.c0 = 0L;
    this.c1 = 0L;
    this.c2 = 0L;
    this.c3 = 0L;
  }

  BidKRaw.Kind rawKind() {
    return switch (kind) {
      case FINITE -> BidKRaw.Kind.FINITE;
      case INF -> BidKRaw.Kind.INF;
      case NAN -> BidKRaw.Kind.NAN;
      case SNAN -> BidKRaw.Kind.SNAN;
    };
  }
}
