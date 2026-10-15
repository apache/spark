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

import java.math.BigDecimal;
import java.math.MathContext;

/** Format table, codec, Decimal96, and wide BID-k arithmetic checks. */
public final class BidKTest {
  private BidKTest() {
  }

  public static void main(String[] args) {
    formatTable();
    LimbMathTest.main(new String[0]);
    codecMatchesBid64();
    codecMatchesBid128();
    decimal96RoundtripAndConvert();
    arithmeticVsBigDecimal(FormatParams.DECIMAL96);
    arithmeticVsBigDecimal(FormatParams.DECIMAL160);
    arithmeticVsBigDecimal(FormatParams.DECIMAL192);
    arithmeticVsBigDecimal(FormatParams.DECIMAL224);
    arithmeticVsBigDecimal(FormatParams.DECIMAL256);
    decimal160Cases();
    convertWideTo128();
    decimal96Dpml();
    wideLibmUnsupported();
    roundingModes();
    sqlAdapters();
    System.out.println("BidKTest passed");
  }

  private static void formatTable() {
    expect(FormatParams.DECIMAL64.precision() == 16, "d64 p");
    expect(FormatParams.DECIMAL64.emax() == 384, "d64 emax");
    expect(FormatParams.DECIMAL64.bias() == 398, "d64 bias");
    expect(FormatParams.DECIMAL96.precision() == 25, "d96 p");
    expect(FormatParams.DECIMAL96.emax() == 1536, "d96 emax");
    expect(FormatParams.DECIMAL96.bias() == 1559, "d96 bias");
    expect(FormatParams.DECIMAL128.precision() == 34, "d128 p");
    expect(FormatParams.DECIMAL128.emax() == 6144, "d128 emax");
    expect(FormatParams.DECIMAL128.bias() == 6176, "d128 bias");
    expect(FormatParams.DECIMAL160.precision() == 43, "d160 p");
    expect(FormatParams.DECIMAL160.emax() == 24576, "d160 emax");
    expect(FormatParams.DECIMAL160.bias() == 24617, "d160 bias");
    expect(FormatParams.DECIMAL192.precision() == 52, "d192 p");
    expect(FormatParams.DECIMAL192.emax() == 98304, "d192 emax");
    expect(FormatParams.DECIMAL192.bias() == 98354, "d192 bias");
    expect(FormatParams.DECIMAL224.precision() == 61, "d224 p");
    expect(FormatParams.DECIMAL224.emax() == 393216, "d224 emax");
    expect(FormatParams.DECIMAL224.bias() == 393275, "d224 bias");
    expect(FormatParams.DECIMAL256.precision() == 70, "d256 p");
    expect(FormatParams.DECIMAL256.emax() == 1572864, "d256 emax");
    expect(FormatParams.DECIMAL256.bias() == 1572932, "d256 bias");
    expect(FormatParams.DECIMAL32.units() == 1, "k=1");
    expect(FormatParams.DECIMAL64.units() == 2, "k=2");
    expect(FormatParams.DECIMAL96.units() == 3, "k=3");
    expect(FormatParams.DECIMAL128.units() == 4, "k=4");
    expect(FormatParams.DECIMAL160.units() == 5, "k=5");
    expect(FormatParams.DECIMAL256.units() == 8, "k=8");
    expect(FormatParams.ofUnits(5) == FormatParams.DECIMAL160, "ofUnits 5");
    expect(FormatParams.DECIMAL96.wordCount() == 2, "d96 words");
    expect(FormatParams.DECIMAL160.wordCount() == 3, "d160 words");
    expect(FormatParams.DECIMAL256.wordCount() == 4, "d256 words");
    expect(FormatParams.DECIMAL160.packedBits() == 160, "d160 bits");
  }

  private static void codecMatchesBid64() {
    long[] cases = {
      Bid64.finite(false, 398, 1L).toRawBits(),
      Bid64.finite(true, 398, 1L).toRawBits(),
      Bid64.finite(false, 0, 1L).toRawBits(),
      Bid64.finite(false, 767, PowersOfTen.MAX_16).toRawBits(),
      Bid64.finite(false, 398, 9_007_199_254_740_992L).toRawBits(),
      Bid64.POSITIVE_INFINITY.toRawBits(),
      Bid64.NEGATIVE_INFINITY.toRawBits(),
      Bid64.QUIET_NAN.toRawBits(),
      Bid64.SIGNALING_NAN.toRawBits(),
      Bid64.NEGATIVE_ZERO.toRawBits()
    };
    FormatParams fmt = FormatParams.DECIMAL64;
    UIntLimbs coeff = new UIntLimbs();
    long[] words = new long[1];
    long[] packed = new long[1];
    for (long bits : cases) {
      words[0] = bits;
      if (BidCodec.isNaN(fmt, words) || BidCodec.isInfinite(fmt, words)) {
        expect(BidCodec.isNaN(fmt, words) == Bid64.fromRawBits(bits).isNaN(), "nan");
        expect(
            BidCodec.isInfinite(fmt, words) == Bid64.fromRawBits(bits).isInfinite(),
            "inf");
        continue;
      }
      int exp = BidCodec.unpackBiasedExponent(fmt, words);
      BidCodec.unpackCoefficient(fmt, words, coeff);
      expect(exp == Bid64.fromRawBits(bits).biasedExponent(), "exp64");
      BidCodec.packFinite(fmt, BidCodec.isSigned(fmt, words), exp, coeff, packed);
      expect(packed[0] == bits, "pack64 " + Long.toHexString(bits));
    }
  }

  private static void codecMatchesBid128() {
    Bid128[] cases = {
      Bid128.finite(false, 6176, 0L, 1L),
      Bid128.finite(true, 6176, 0L, 1L),
      Bid128.finite(false, 0, 0L, 1L),
      Bid128.POSITIVE_INFINITY,
      Bid128.QUIET_NAN,
      Bid128.NEGATIVE_ZERO
    };
    FormatParams fmt = FormatParams.DECIMAL128;
    UIntLimbs coeff = new UIntLimbs();
    long[] packed = new long[2];
    for (Bid128 value : cases) {
      long[] words = {value.lowBits(), value.highBits()};
      if (!BidCodec.isFinite(fmt, words)) {
        expect(BidCodec.isNaN(fmt, words) == value.isNaN(), "nan128");
        expect(BidCodec.isInfinite(fmt, words) == value.isInfinite(), "inf128");
        continue;
      }
      int exp = BidCodec.unpackBiasedExponent(fmt, words);
      BidCodec.unpackCoefficient(fmt, words, coeff);
      expect(exp == value.biasedExponent(), "exp128");
      BidCodec.packFinite(fmt, value.isSigned(), exp, coeff, packed);
      expect(packed[0] == value.lowBits() && packed[1] == value.highBits(), "pack128");
    }
  }

  private static void decimal96RoundtripAndConvert() {
    FormatParams d96 = FormatParams.DECIMAL96;
    StatusFlags flags = new StatusFlags();
    BidK value = BidK.parse(d96, "1.234567890123456789012345e10");
    BidK again = BidK.parse(d96, value.toString());
    expect(value.equals(again), "d96 string roundtrip");
    Bid128 wide = value.toBid128(RoundingMode.TIES_TO_EVEN, flags);
    BidK back = BidK.fromBid128(wide, d96, RoundingMode.TIES_TO_EVEN, flags);
    expect(value.sqlEquals(back), "d96 via bid128");
    Bid64 narrow = value.toBid64(RoundingMode.TIES_TO_EVEN, flags);
    expect(narrow.isFinite(), "d96 to d64 finite");
  }

  private static void arithmeticVsBigDecimal(FormatParams format) {
    MathContext mc = new MathContext(
        format.precision(), java.math.RoundingMode.HALF_EVEN);
    String[] samples = {
      "1",
      "-2.5",
      "0.0001",
      "1234567890123456789012345",
      "9.999e-10",
      "3.141592653589793238462643"
    };
    RoundingMode mode = RoundingMode.TIES_TO_EVEN;
    StatusFlags flags = new StatusFlags();
    for (String left : samples) {
      for (String right : samples) {
        BidK x = BidK.parse(format, left);
        BidK y = BidK.parse(format, right);
        BigDecimal bx = new BigDecimal(left);
        BigDecimal by = new BigDecimal(right);
        expectClose(x.add(y, mode, flags), bx.add(by, mc), format, "add");
        expectClose(x.subtract(y, mode, flags), bx.subtract(by, mc), format, "sub");
        expectClose(x.multiply(y, mode, flags), bx.multiply(by, mc), format, "mul");
        if (by.compareTo(BigDecimal.ZERO) != 0) {
          expectClose(x.divide(y, mode, flags), bx.divide(by, mc), format, "div");
        }
      }
      BidK x = BidK.parse(format, left);
      BigDecimal bx = new BigDecimal(left);
      if (bx.signum() >= 0) {
        expectClose(x.sqrt(mode, flags), bx.sqrt(mc), format, "sqrt");
      }
    }
  }

  private static void decimal160Cases() {
    FormatParams d160 = FormatParams.DECIMAL160;
    RoundingMode mode = RoundingMode.TIES_TO_EVEN;
    StatusFlags flags = new StatusFlags();
    MathContext mc = new MathContext(43, java.math.RoundingMode.HALF_EVEN);
    String[] extra = {
      "9999999999999999999999999999999999999999999",
      "1.000000000000000000000000000000000000000001",
      "2.5e20",
      "1e-20",
      "1234567890123456789012345678901234567890123"
    };
    for (String left : extra) {
      for (String right : extra) {
        BidK x = BidK.parse(d160, left);
        BidK y = BidK.parse(d160, right);
        BigDecimal bx = new BigDecimal(left);
        BigDecimal by = new BigDecimal(right);
        expectClose(x.add(y, mode, flags), bx.add(by, mc), d160, "d160 add");
        expectClose(x.multiply(y, mode, flags), bx.multiply(by, mc), d160, "d160 mul");
        if (by.compareTo(BigDecimal.ZERO) != 0) {
          expectClose(x.divide(y, mode, flags), bx.divide(by, mc), d160, "d160 div");
        }
      }
    }
    BidK two = BidK.parse(d160, "2");
    BidK three = BidK.parse(d160, "3");
    BidK q = two.divide(three, mode, flags);
    expectClose(q, new BigDecimal("2").divide(new BigDecimal("3"), mc), d160, "2/3");
    BidK max = BidK.parse(d160, extra[0]);
    BidK one = BidK.parse(d160, "1");
    expectClose(
        max.add(one, mode, flags),
        new BigDecimal(extra[0]).add(BigDecimal.ONE, mc),
        d160,
        "max+1");
    BidK x = BidK.parse(d160, "1.25");
    BidK y = BidK.parse(d160, "1.00");
    BidK qz = x.quantize(y, mode, flags);
    expectClose(qz, new BigDecimal("1.25"), d160, "d160 quantize");
    BidK a = BidK.parse(d160, "1.5");
    BidK b = BidK.parse(d160, "2");
    BidK c = BidK.parse(d160, "0.25");
    expectClose(
        a.fma(b, c, mode, flags),
        new BigDecimal("1.5").multiply(new BigDecimal("2")).add(new BigDecimal("0.25"), mc),
        d160,
        "d160 fma");
    Bid128 d128 = x.toBid128(mode, flags);
    BidK back = BidK.fromBid128(d128, d160, mode, flags);
    expect(x.sqlEquals(back), "d160 via d128");
    for (RoundingMode rm : RoundingMode.values()) {
      StatusFlags st = new StatusFlags();
      BidK r = two.divide(three, rm, st);
      expect(r.isFinite(), "d160 2/3 " + rm);
    }
  }

  private static void convertWideTo128() {
    StatusFlags flags = new StatusFlags();
    BidK wide = BidK.parse(FormatParams.DECIMAL256, "123456789012345678901234567890");
    Bid128 d128 = wide.toBid128(RoundingMode.TIES_TO_EVEN, flags);
    expect(d128.isFinite(), "d256 to d128");
    FormatParams d256 = FormatParams.DECIMAL256;
    MathContext mc = new MathContext(70, java.math.RoundingMode.HALF_EVEN);
    String full = "1234567890123456789012345678901234567890123456789012345678901234567890";
    BidK x = BidK.parse(d256, full);
    expectClose(
        x.sqrt(RoundingMode.TIES_TO_EVEN, flags),
        new BigDecimal(full).sqrt(mc),
        d256,
        "d256 sqrt 70-digit");
  }

  private static void decimal96Dpml() {
    StatusFlags flags = new StatusFlags();
    BidK x = BidK.parse(FormatParams.DECIMAL96, "0");
    BidK y = x.exp(RoundingMode.TIES_TO_EVEN, flags);
    expect(y.toString().contains("1"), "exp(0)~1");
  }

  private static void wideLibmUnsupported() {
    BidK x = BidK.parse(FormatParams.DECIMAL160, "1");
    try {
      x.exp(RoundingMode.TIES_TO_EVEN, new StatusFlags());
      throw new IllegalStateException("expected unsupported libm");
    } catch (UnsupportedOperationException expected) {
      expect(expected.getMessage().contains("k>128"), "libm message");
    }
  }

  private static void roundingModes() {
    FormatParams format = FormatParams.DECIMAL96;
    BidK two = BidK.parse(format, "2");
    BidK three = BidK.parse(format, "3");
    for (RoundingMode mode : RoundingMode.values()) {
      StatusFlags flags = new StatusFlags();
      BidK q = two.divide(three, mode, flags);
      expect(q.isFinite(), "2/3 " + mode);
    }
  }

  private static void sqlAdapters() {
    FormatParams[] formats = {
      FormatParams.DECIMAL96,
      FormatParams.DECIMAL160,
      FormatParams.DECIMAL256
    };
    for (FormatParams format : formats) {
      BidK n0 = BidK.parse(format, "-0");
      BidK p0 = BidK.parse(format, "0");
      expect(n0.sqlEquals(p0), "sql zeros " + format);
      BidK nan = BidK.naN(format, false, false);
      expect(nan.sqlCompare(p0) > 0, "sql nan greatest " + format);
      expect(nan.sqlEquals(BidK.naN(format, true, true)), "sql nan equal " + format);
      BidK a = BidK.parse(format, "1.25e10");
      BidK b = BidK.parse(format, "2.5e9");
      expect(a.sqlCompare(b) > 0, "sql order " + format);
      BidK c = BidK.parse(format, "125e8");
      expect(a.sqlEquals(c), "sql cohort " + format);
    }
  }

  private static void expectClose(BidK got, BigDecimal expected, FormatParams format, String op) {
    if (!got.isFinite()) {
      throw new IllegalStateException(op + " not finite: " + got + " fmt=" + format);
    }
    BigDecimal actual = new BigDecimal(stripSign(got.toString()));
    BigDecimal ulp = BigDecimal.ONE.scaleByPowerOfTen(-format.precision() + 2);
    BigDecimal delta = actual.subtract(expected).abs();
    BigDecimal scale = expected.abs().max(BigDecimal.ONE);
    if (delta.compareTo(scale.multiply(ulp)) > 0) {
      throw new IllegalStateException(
          op + " " + format + " got " + actual + " expected " + expected);
    }
  }

  private static String stripSign(String text) {
    if (text.startsWith("+")) {
      return text.substring(1);
    }
    return text;
  }

  private static void expect(boolean condition, String label) {
    if (!condition) {
      throw new IllegalStateException(label);
    }
  }
}
