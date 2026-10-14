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
 * IEEE 754-2008 decimal interchange format parameters for packed width
 * {@code k} bits, where {@code k} is a multiple of 32 in {@code [32, 256]}.
 *
 * <p>{@code p = 9*(k/32)-2}, {@code w = k/16+4}, {@code t = k-w-6},
 * {@code emax = 3*2^(k/16+3)}, {@code bias = emax+p-2}.
 */
public final class FormatParams {
  private static final FormatParams[] BY_K = build();

  public static final FormatParams DECIMAL32 = BY_K[1];
  public static final FormatParams DECIMAL64 = BY_K[2];
  public static final FormatParams DECIMAL96 = BY_K[3];
  public static final FormatParams DECIMAL128 = BY_K[4];
  public static final FormatParams DECIMAL160 = BY_K[5];
  public static final FormatParams DECIMAL192 = BY_K[6];
  public static final FormatParams DECIMAL224 = BY_K[7];
  public static final FormatParams DECIMAL256 = BY_K[8];

  private static FormatParams[] build() {
    FormatParams[] byK = new FormatParams[9];
    for (int k = 32; k <= 256; k += 32) {
      byK[k / 32] = compute(k);
    }
    return byK;
  }

  private final int k;
  private final int precision;
  private final int w;
  private final int t;
  private final int emax;
  private final int emin;
  private final int bias;
  private final int maxBiasedExponent;
  private final int expBits;
  private final int form1CoeffBits;
  private final int form2CoeffBits;
  private final int wordCount;
  private final int signBit;
  private final long[] maxCoefficient;

  private FormatParams(
      int k,
      int precision,
      int w,
      int t,
      int emax,
      int emin,
      int bias,
      int maxBiasedExponent,
      int expBits,
      int form1CoeffBits,
      int form2CoeffBits,
      int wordCount,
      int signBit,
      long[] maxCoefficient) {
    this.k = k;
    this.precision = precision;
    this.w = w;
    this.t = t;
    this.emax = emax;
    this.emin = emin;
    this.bias = bias;
    this.maxBiasedExponent = maxBiasedExponent;
    this.expBits = expBits;
    this.form1CoeffBits = form1CoeffBits;
    this.form2CoeffBits = form2CoeffBits;
    this.wordCount = wordCount;
    this.signBit = signBit;
    this.maxCoefficient = maxCoefficient;
  }

  /** Returns the descriptor for packed width {@code k} bits. */
  public static FormatParams of(int k) {
    if (k < 32 || k > 256 || (k & 31) != 0) {
      throw new IllegalArgumentException("k must be a multiple of 32 in [32, 256]");
    }
    return BY_K[k / 32];
  }

  private static FormatParams compute(int k) {
    int precision = 9 * (k / 32) - 2;
    int w = k / 16 + 4;
    int t = k - w - 6;
    int emax = 3 << (k / 16 + 3);
    int emin = 1 - emax;
    int bias = emax + precision - 2;
    int maxBiasedExponent = 2 * emax - 1;
    int expBits = w + 2;
    int form1CoeffBits = t + 3;
    int form2CoeffBits = t + 1;
    int wordCount = (k + 63) / 64;
    int signBit = k - 1;
    long[] maxCoefficient = UIntLimbs.pow10MinusOne(precision);
    return new FormatParams(
        k,
        precision,
        w,
        t,
        emax,
        emin,
        bias,
        maxBiasedExponent,
        expBits,
        form1CoeffBits,
        form2CoeffBits,
        wordCount,
        signBit,
        maxCoefficient);
  }

  public int packedBits() {
    return k;
  }

  public int precision() {
    return precision;
  }

  public int combinationExponentBits() {
    return w;
  }

  public int trailingSignificandBits() {
    return t;
  }

  public int emax() {
    return emax;
  }

  public int emin() {
    return emin;
  }

  public int bias() {
    return bias;
  }

  public int maxBiasedExponent() {
    return maxBiasedExponent;
  }

  public int minQuantumExponent() {
    return -bias;
  }

  public int maxQuantumExponent() {
    return maxBiasedExponent - bias;
  }

  int expBits() {
    return expBits;
  }

  int form1CoeffBits() {
    return form1CoeffBits;
  }

  int form2CoeffBits() {
    return form2CoeffBits;
  }

  /** Little-endian {@code long} words needed for a packed encoding. */
  public int wordCount() {
    return wordCount;
  }

  int signBit() {
    return signBit;
  }

  long[] maxCoefficientWords() {
    return maxCoefficient;
  }

  boolean coefficientAllowed(UIntLimbs coefficient) {
    return coefficient.compare(maxCoefficient) <= 0;
  }

  /** Whether this format's coefficient and exponent fit in BID128. */
  public boolean fitsInDecimal128() {
    return k <= 128;
  }

  @Override
  public String toString() {
    return "decimal" + k;
  }
}
