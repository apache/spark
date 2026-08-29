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

import java.lang.reflect.Method;
import java.math.BigDecimal;

/** Checks that object APIs preserve raw-kernel values and flags. */
public final class BidObjectApiTest {
  private static final String[] UNARY = {
      "exp", "expm1", "exp2", "exp10", "log", "log10", "log2", "log1p",
      "sin", "cos", "tan", "asin", "acos", "atan",
      "sinh", "cosh", "tanh", "asinh", "acosh", "atanh",
      "erf", "erfc", "tgamma", "lgamma", "cbrt"
  };
  private static final String[] BINARY = {"pow", "hypot", "atan2"};
  private static final RoundingMode[] MODES = {
      RoundingMode.TIES_TO_EVEN, RoundingMode.TOWARD_ZERO
  };

  private BidObjectApiTest() {
  }

  public static void main(String[] args) {
    testBid64();
    testBid128();
    testArithmeticParity();
    testTranscendentalParity();
    testCompatStatus();
    System.out.println("BidObjectApiTest: all tests passed");
  }

  private static void testBid64() {
    StatusFlags flags = new StatusFlags();
    Bid64 x = Bid64.parse("12.75", RoundingMode.TIES_TO_EVEN, flags);
    Bid64 y = Bid64.parseExact("2.5");
    check(x.compareTo(y) > 0, "compareTo64");
    check(Bid64.NEGATIVE_ZERO.compareTo(Bid64.POSITIVE_ZERO) < 0, "signedZeroOrder64");
    Bid64 zero = Bid64.finite(false, 0, 0);
    Bid64 noncanonicalZero =
        Bid64.fromRawBits(Bid64.finiteRawBits(false, 0, 10_000_000_000_000_000L));
    check(Integer.signum(zero.compareTo(noncanonicalZero))
        == -Integer.signum(noncanonicalZero.compareTo(zero)), "strictOrder64");
    check(Bid64.fromLong(Long.MIN_VALUE, RoundingMode.TIES_TO_EVEN, flags).toRawBits()
        == Bid64Raw.fromInt64(Long.MIN_VALUE, RoundingMode.TIES_TO_EVEN, new StatusFlags()),
        "fromLong64");
    StatusFlags integerFlags = new StatusFlags();
    check(x.toLong(RoundingMode.TOWARD_ZERO, integerFlags) == 12L, "long64");
    check(integerFlags.contains(StatusFlags.INEXACT), "longInexact64");
    check(x.toCanonicalString().equals("1275E-2"), "parse64");
    check(x.toBigDecimal().equals(new BigDecimal("12.75")), "bigDecimal64");
    check(Bid64.fromBigDecimalExact(new BigDecimal("12.750")).toCanonicalString()
        .equals("12750E-3"), "bigDecimalExact64");
    StatusFlags decimalFlags = new StatusFlags();
    Bid64.fromBigDecimal(
        new BigDecimal("1.2345678901234567"),
        RoundingMode.TIES_TO_EVEN,
        decimalFlags);
    check(decimalFlags.contains(StatusFlags.INEXACT), "bigDecimalInexact64");
    boolean rejected = false;
    try {
      Bid64.fromBigDecimalExact(new BigDecimal("1e1000"));
    } catch (ArithmeticException expected) {
      rejected = true;
    }
    check(rejected, "bigDecimalRange64");
    check(x.toDouble(RoundingMode.TIES_TO_EVEN, flags) == 12.75, "double64");
    check(x.toFloat(RoundingMode.TIES_TO_EVEN, flags) == 12.75f, "float64");
    check(x.toBid128(flags).toBid64(RoundingMode.TIES_TO_EVEN, flags)
        .quietEqual(x, new StatusFlags()), "widen64");
    check(x.roundIntegral(RoundingMode.TIES_TO_EVEN, false, flags)
        .quietEqual(Bid64.parseExact("13"), new StatusFlags()), "round64");
    check(x.scaleByPowerOfTen(1, RoundingMode.TIES_TO_EVEN, flags)
        .quietEqual(Bid64.parseExact("127.5"), new StatusFlags()), "scale64");
    check(x.fmod(y, flags).quietEqual(
        Bid64.fromRawBits(Bid64Raw.fmod(
            x.toRawBits(), y.toRawBits(), new StatusFlags())),
        new StatusFlags()), "fmod64");
    check(x.nextAfter(y, flags).toRawBits()
        == Bid64Raw.nextAfter(x.toRawBits(), y.toRawBits(), new StatusFlags()),
        "nextAfter64");
    check(x.minNum(y, flags).quietEqual(y, new StatusFlags()), "min64");
    check(x.maxNum(y, flags).quietEqual(x, new StatusFlags()), "max64");
    check(x.minNumMagnitude(y, flags).quietEqual(y, new StatusFlags()), "minMag64");
    check(x.maxNumMagnitude(y, flags).quietEqual(x, new StatusFlags()), "maxMag64");
    check(x.quantum().toRawBits() == Bid64Raw.quantum(x.toRawBits()), "quantum64");
    check(x.quantumExponent(flags) == Bid64Raw.quantexp(x.toRawBits()), "quantexp64");
    check(x.ilogb(flags) == Bid64Raw.ilogb(x.toRawBits(), new StatusFlags()), "ilogb64");
    check(x.logb(flags).toRawBits()
        == Bid64Raw.logb(x.toRawBits(), new StatusFlags()), "logb64");
    check(Bid64.QUIET_NAN.quietGreaterUnordered(x, flags), "greaterUnordered64");
    check(Bid64.QUIET_NAN.quietLessUnordered(x, flags), "lessUnordered64");
    check(Bid64.QUIET_NAN.quietNotGreater(x, flags), "notGreater64");
    check(Bid64.QUIET_NAN.quietNotLess(x, flags), "notLess64");
  }

  private static void testBid128() {
    StatusFlags flags = new StatusFlags();
    Bid128 x = Bid128.parse("12.75", RoundingMode.TIES_TO_EVEN, flags);
    Bid128 y = Bid128.parseExact("2.5");
    check(x.compareTo(y) > 0, "compareTo128");
    check(Bid128.NEGATIVE_ZERO.compareTo(Bid128.POSITIVE_ZERO) < 0, "signedZeroOrder128");
    Bid128 zero = Bid128.rawFinite(false, 0, 0, 0);
    Bid128 noncanonicalZero = Bid128.fromRawBits(0x6000_0000_0000_0000L, 1L);
    check(Integer.signum(zero.compareTo(noncanonicalZero))
        == -Integer.signum(noncanonicalZero.compareTo(zero)), "strictOrder128");
    check(x.toCanonicalString().equals("1275E-2"), "parse128");
    check(x.toBigDecimal().equals(new BigDecimal("12.75")), "bigDecimal128");
    check(Bid128.fromBigDecimalExact(new BigDecimal("12.750")).toCanonicalString()
        .equals("12750E-3"), "bigDecimalExact128");
    StatusFlags decimalFlags = new StatusFlags();
    Bid128.fromBigDecimal(
        new BigDecimal("1.2345678901234567890123456789012345"),
        RoundingMode.TIES_TO_EVEN,
        decimalFlags);
    check(decimalFlags.contains(StatusFlags.INEXACT), "bigDecimalInexact128");
    boolean rejected = false;
    try {
      Bid128.fromBigDecimalExact(new BigDecimal("1e10000"));
    } catch (ArithmeticException expected) {
      rejected = true;
    }
    check(rejected, "bigDecimalRange128");
    StatusFlags integerFlags = new StatusFlags();
    check(x.toLong(RoundingMode.TOWARD_ZERO, integerFlags) == 12L, "long128");
    check(integerFlags.contains(StatusFlags.INEXACT), "longInexact128");
    check(x.toDouble(RoundingMode.TIES_TO_EVEN, flags) == 12.75, "double128");
    check(x.toFloat(RoundingMode.TIES_TO_EVEN, flags) == 12.75f, "float128");
    check(x.toBid64(RoundingMode.TIES_TO_EVEN, flags)
        .quietEqual(Bid64.parseExact("12.75"), new StatusFlags()), "narrow128");
    check(Bid128.fromLong(12L, RoundingMode.TIES_TO_EVEN, flags)
        .quietEqual(Bid128.parseExact("12"), new StatusFlags()), "fromLong128");
    check(x.roundIntegral(RoundingMode.TIES_TO_EVEN, false, flags)
        .quietEqual(Bid128.parseExact("13"), new StatusFlags()), "round128");
    check(x.scaleByPowerOfTen(1, RoundingMode.TIES_TO_EVEN, flags)
        .quietEqual(Bid128.parseExact("127.5"), new StatusFlags()), "scale128");
    check(x.fmod(y, flags).quietEqual(rawFmod128(x, y), new StatusFlags()), "fmod128");
    check(x.nextAfter(y, flags).equals(rawNextAfter128(x, y)), "nextAfter128");
    check(x.minNum(y, flags).quietEqual(y, new StatusFlags()), "min128");
    check(x.maxNum(y, flags).quietEqual(x, new StatusFlags()), "max128");
    check(x.minNumMagnitude(y, flags).quietEqual(y, new StatusFlags()), "minMag128");
    check(x.maxNumMagnitude(y, flags).quietEqual(x, new StatusFlags()), "maxMag128");
    check(x.quantum().equals(rawQuantum128(x)), "quantum128");
    check(x.quantumExponent(flags)
        == Bid128Raw.quantexp(x.highBits(), x.lowBits()), "quantexp128");
    check(x.ilogb(flags)
        == Bid128Raw.ilogb(x.highBits(), x.lowBits(), new StatusFlags()), "ilogb128");
    check(x.logb(flags).equals(rawLogb128(x)), "logb128");
    check(Bid128.QUIET_NAN.quietGreaterUnordered(x, flags), "greaterUnordered128");
    check(Bid128.QUIET_NAN.quietLessUnordered(x, flags), "lessUnordered128");
    check(Bid128.QUIET_NAN.quietNotGreater(x, flags), "notGreater128");
    check(Bid128.QUIET_NAN.quietNotLess(x, flags), "notLess128");
  }

  private static void testArithmeticParity() {
    Bid64[] samples64 = {
        Bid64.parseExact("0"),
        Bid64.parseExact("0.5"),
        Bid64.parseExact("1"),
        Bid64.parseExact("12.75"),
        Bid64.parseExact("-2.5"),
        Bid64.POSITIVE_INFINITY,
        Bid64.NEGATIVE_INFINITY,
        Bid64.QUIET_NAN,
        Bid64.SIGNALING_NAN,
        Bid64.POSITIVE_ZERO,
        Bid64.NEGATIVE_ZERO
    };
    Bid128[] samples128 = {
        Bid128.parseExact("0"),
        Bid128.parseExact("0.5"),
        Bid128.parseExact("1"),
        Bid128.parseExact("12.75"),
        Bid128.parseExact("-2.5"),
        Bid128.POSITIVE_INFINITY,
        Bid128.NEGATIVE_INFINITY,
        Bid128.QUIET_NAN,
        Bid128.SIGNALING_NAN,
        Bid128.POSITIVE_ZERO,
        Bid128.NEGATIVE_ZERO
    };
    Bid64[] rhs64 = {
        Bid64.parseExact("1"),
        Bid64.parseExact("2.5"),
        Bid64.parseExact("-1"),
        Bid64.POSITIVE_ZERO,
        Bid64.POSITIVE_INFINITY,
        Bid64.QUIET_NAN
    };
    Bid128[] rhs128 = {
        Bid128.parseExact("1"),
        Bid128.parseExact("2.5"),
        Bid128.parseExact("-1"),
        Bid128.POSITIVE_ZERO,
        Bid128.POSITIVE_INFINITY,
        Bid128.QUIET_NAN
    };
    for (RoundingMode mode : MODES) {
      for (Bid64 x : samples64) {
        checkUnaryRounded64("sqrt", x, mode);
        checkUnaryRounded64("cbrt", x, mode);
        checkNext64(x);
        checkRound64(x, mode);
        for (Bid64 y : rhs64) {
          checkBinaryRounded64("add", "add", x, y, mode);
          checkBinaryRounded64("subtract", "sub", x, y, mode);
          checkBinaryRounded64("multiply", "mul", x, y, mode);
          checkBinaryRounded64("divide", "div", x, y, mode);
          checkBinaryFlags64("remainder", "rem", x, y);
          checkBinaryFlags64("fmod", "fmod", x, y);
          checkFma64(x, y, y, mode);
          checkQuantize64(x, y, mode);
          checkFdim64(x, y, mode);
        }
      }
      for (Bid128 x : samples128) {
        checkUnaryRounded128("sqrt", x, mode);
        checkUnaryRounded128("cbrt", x, mode);
        checkNext128(x);
        checkRound128(x, mode);
        for (Bid128 y : rhs128) {
          checkBinaryRounded128("add", "add", x, y, mode);
          checkBinaryRounded128("subtract", "sub", x, y, mode);
          checkBinaryRounded128("multiply", "mul", x, y, mode);
          checkBinaryRounded128("divide", "div", x, y, mode);
          checkBinaryFlags128("remainder", "rem", x, y);
          checkBinaryFlags128("fmod", "fmod", x, y);
          checkFma128(x, y, y, mode);
          checkQuantize128(x, y, mode);
          checkFdim128(x, y, mode);
        }
      }
    }
  }

  private static void testTranscendentalParity() {
    Bid64[] samples64 = {
        Bid64.parseExact("0"),
        Bid64.parseExact("0.5"),
        Bid64.parseExact("1"),
        Bid64.parseExact("2"),
        Bid64.parseExact("-0.5"),
        Bid64.parseExact("3"),
        Bid64.POSITIVE_INFINITY,
        Bid64.NEGATIVE_INFINITY,
        Bid64.QUIET_NAN,
        Bid64.SIGNALING_NAN
    };
    Bid128[] samples128 = {
        Bid128.parseExact("0"),
        Bid128.parseExact("0.5"),
        Bid128.parseExact("1"),
        Bid128.parseExact("2"),
        Bid128.parseExact("-0.5"),
        Bid128.parseExact("3"),
        Bid128.POSITIVE_INFINITY,
        Bid128.NEGATIVE_INFINITY,
        Bid128.QUIET_NAN,
        Bid128.SIGNALING_NAN
    };
    check(samples64.length == samples128.length, "sampleCount");
    check(UNARY.length == 25, "unaryCount");
    check(BINARY.length == 3, "binaryCount");

    for (String name : UNARY) {
      for (RoundingMode mode : MODES) {
        for (int i = 0; i < samples64.length; i++) {
          checkUnary64(name, samples64[i], mode);
          checkUnary128(name, samples128[i], mode);
        }
      }
    }
    Bid64[] rhs64 = {
        Bid64.parseExact("0"),
        Bid64.parseExact("1"),
        Bid64.parseExact("3"),
        Bid64.parseExact("-1"),
        Bid64.POSITIVE_INFINITY,
        Bid64.QUIET_NAN
    };
    Bid128[] rhs128 = {
        Bid128.parseExact("0"),
        Bid128.parseExact("1"),
        Bid128.parseExact("3"),
        Bid128.parseExact("-1"),
        Bid128.POSITIVE_INFINITY,
        Bid128.QUIET_NAN
    };
    for (String name : BINARY) {
      for (RoundingMode mode : MODES) {
        for (Bid64 left : samples64) {
          for (Bid64 right : rhs64) {
            checkBinary64(name, left, right, mode);
          }
        }
        for (Bid128 left : samples128) {
          for (Bid128 right : rhs128) {
            checkBinary128(name, left, right, mode);
          }
        }
      }
    }
  }

  private static void checkUnaryRounded64(String name, Bid64 x, RoundingMode mode) {
    checkUnary64(name, x, mode);
  }

  private static void checkUnaryRounded128(String name, Bid128 x, RoundingMode mode) {
    checkUnary128(name, x, mode);
  }

  private static void checkBinaryRounded64(
      String objectName, String rawName, Bid64 left, Bid64 right, RoundingMode mode) {
    try {
      Method object = Bid64.class.getMethod(
          objectName, Bid64.class, RoundingMode.class, StatusFlags.class);
      Method raw = Bid64Raw.class.getMethod(
          rawName, long.class, long.class, RoundingMode.class, StatusFlags.class);
      StatusFlags objectFlags = new StatusFlags();
      StatusFlags rawFlags = new StatusFlags();
      Bid64 objectResult = (Bid64) object.invoke(left, right, mode, objectFlags);
      long rawResult = (Long) raw.invoke(
          null, left.toRawBits(), right.toRawBits(), mode, rawFlags);
      check(
          objectResult.toRawBits() == rawResult,
          "arith64Bits:" + objectName);
      check(objectFlags.bits() == rawFlags.bits(), "arith64Flags:" + objectName);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("arith64:" + objectName, e);
    }
  }

  private static void checkBinaryRounded128(
      String objectName, String rawName, Bid128 left, Bid128 right, RoundingMode mode) {
    try {
      Method object = Bid128.class.getMethod(
          objectName, Bid128.class, RoundingMode.class, StatusFlags.class);
      Method raw = Bid128Raw.class.getMethod(
          rawName,
          long.class,
          long.class,
          long.class,
          long.class,
          RoundingMode.class,
          StatusFlags.class,
          long[].class);
      StatusFlags objectFlags = new StatusFlags();
      StatusFlags rawFlags = new StatusFlags();
      Bid128 objectResult =
          (Bid128) object.invoke(left, right, mode, objectFlags);
      long[] rawBits = new long[2];
      raw.invoke(
          null,
          left.highBits(),
          left.lowBits(),
          right.highBits(),
          right.lowBits(),
          mode,
          rawFlags,
          rawBits);
      check(
          objectResult.highBits() == rawBits[0]
              && objectResult.lowBits() == rawBits[1],
          "arith128Bits:" + objectName);
      check(
          objectFlags.bits() == rawFlags.bits(), "arith128Flags:" + objectName);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("arith128:" + objectName, e);
    }
  }

  private static void checkBinaryFlags64(
      String objectName, String rawName, Bid64 left, Bid64 right) {
    try {
      Method object = Bid64.class.getMethod(
          objectName, Bid64.class, StatusFlags.class);
      Method raw = Bid64Raw.class.getMethod(
          rawName, long.class, long.class, StatusFlags.class);
      StatusFlags objectFlags = new StatusFlags();
      StatusFlags rawFlags = new StatusFlags();
      Bid64 objectResult = (Bid64) object.invoke(left, right, objectFlags);
      long rawResult = (Long) raw.invoke(
          null, left.toRawBits(), right.toRawBits(), rawFlags);
      check(
          objectResult.toRawBits() == rawResult,
          "flags64Bits:" + objectName);
      check(objectFlags.bits() == rawFlags.bits(), "flags64Flags:" + objectName);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("flags64:" + objectName, e);
    }
  }

  private static void checkBinaryFlags128(
      String objectName, String rawName, Bid128 left, Bid128 right) {
    try {
      Method object = Bid128.class.getMethod(
          objectName, Bid128.class, StatusFlags.class);
      Method raw = Bid128Raw.class.getMethod(
          rawName,
          long.class,
          long.class,
          long.class,
          long.class,
          StatusFlags.class,
          long[].class);
      StatusFlags objectFlags = new StatusFlags();
      StatusFlags rawFlags = new StatusFlags();
      Bid128 objectResult = (Bid128) object.invoke(left, right, objectFlags);
      long[] rawBits = new long[2];
      raw.invoke(
          null,
          left.highBits(),
          left.lowBits(),
          right.highBits(),
          right.lowBits(),
          rawFlags,
          rawBits);
      check(
          objectResult.highBits() == rawBits[0]
              && objectResult.lowBits() == rawBits[1],
          "flags128Bits:" + objectName);
      check(
          objectFlags.bits() == rawFlags.bits(), "flags128Flags:" + objectName);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("flags128:" + objectName, e);
    }
  }

  private static void checkFma64(Bid64 x, Bid64 y, Bid64 z, RoundingMode mode) {
    StatusFlags objectFlags = new StatusFlags();
    StatusFlags rawFlags = new StatusFlags();
    long objectBits = x.fma(y, z, mode, objectFlags).toRawBits();
    long rawBits = Bid64Raw.fma(
        x.toRawBits(), y.toRawBits(), z.toRawBits(), mode, rawFlags);
    check(objectBits == rawBits, "fma64Bits");
    check(objectFlags.bits() == rawFlags.bits(), "fma64Flags");
  }

  private static void checkFma128(
      Bid128 x, Bid128 y, Bid128 z, RoundingMode mode) {
    StatusFlags objectFlags = new StatusFlags();
    StatusFlags rawFlags = new StatusFlags();
    Bid128 objectResult = x.fma(y, z, mode, objectFlags);
    long[] rawBits = new long[2];
    Bid128Raw.fma(
        x.highBits(), x.lowBits(), y.highBits(), y.lowBits(),
        z.highBits(), z.lowBits(), mode, rawFlags, rawBits);
    check(
        objectResult.highBits() == rawBits[0]
            && objectResult.lowBits() == rawBits[1],
        "fma128Bits");
    check(objectFlags.bits() == rawFlags.bits(), "fma128Flags");
  }

  private static void checkQuantize64(Bid64 x, Bid64 y, RoundingMode mode) {
    StatusFlags objectFlags = new StatusFlags();
    StatusFlags rawFlags = new StatusFlags();
    long objectBits = x.quantize(y, mode, objectFlags).toRawBits();
    long rawBits = Bid64Raw.quantize(
        x.toRawBits(), y.toRawBits(), mode, rawFlags);
    check(objectBits == rawBits, "quantize64Bits");
    check(objectFlags.bits() == rawFlags.bits(), "quantize64Flags");
  }

  private static void checkQuantize128(Bid128 x, Bid128 y, RoundingMode mode) {
    StatusFlags objectFlags = new StatusFlags();
    StatusFlags rawFlags = new StatusFlags();
    Bid128 objectResult = x.quantize(y, mode, objectFlags);
    long[] rawBits = new long[2];
    Bid128Raw.quantize(
        x.highBits(), x.lowBits(), y.highBits(), y.lowBits(),
        mode, rawFlags, rawBits);
    check(
        objectResult.highBits() == rawBits[0]
            && objectResult.lowBits() == rawBits[1],
        "quantize128Bits");
    check(objectFlags.bits() == rawFlags.bits(), "quantize128Flags");
  }

  private static void checkFdim64(Bid64 x, Bid64 y, RoundingMode mode) {
    StatusFlags objectFlags = new StatusFlags();
    StatusFlags rawFlags = new StatusFlags();
    long objectBits = x.positiveDifference(y, mode, objectFlags).toRawBits();
    long rawBits = Bid64Raw.fdim(x.toRawBits(), y.toRawBits(), mode, rawFlags);
    check(objectBits == rawBits, "fdim64Bits");
    check(objectFlags.bits() == rawFlags.bits(), "fdim64Flags");
  }

  private static void checkFdim128(Bid128 x, Bid128 y, RoundingMode mode) {
    StatusFlags objectFlags = new StatusFlags();
    StatusFlags rawFlags = new StatusFlags();
    Bid128 objectResult = x.positiveDifference(y, mode, objectFlags);
    long[] rawBits = new long[2];
    Bid128Raw.fdim(
        x.highBits(), x.lowBits(), y.highBits(), y.lowBits(),
        mode, rawFlags, rawBits);
    check(
        objectResult.highBits() == rawBits[0]
            && objectResult.lowBits() == rawBits[1],
        "fdim128Bits");
    check(objectFlags.bits() == rawFlags.bits(), "fdim128Flags");
  }

  private static void checkNext64(Bid64 x) {
    StatusFlags upObject = new StatusFlags();
    StatusFlags upRaw = new StatusFlags();
    check(
        x.nextUp(upObject).toRawBits() == Bid64Raw.nextUp(x.toRawBits(), upRaw),
        "nextUp64Bits");
    check(upObject.bits() == upRaw.bits(), "nextUp64Flags");
    StatusFlags downObject = new StatusFlags();
    StatusFlags downRaw = new StatusFlags();
    check(
        x.nextDown(downObject).toRawBits()
            == Bid64Raw.nextDown(x.toRawBits(), downRaw),
        "nextDown64Bits");
    check(downObject.bits() == downRaw.bits(), "nextDown64Flags");
  }

  private static void checkNext128(Bid128 x) {
    StatusFlags upObject = new StatusFlags();
    StatusFlags upRaw = new StatusFlags();
    long[] up = new long[2];
    Bid128Raw.nextUp(x.highBits(), x.lowBits(), upRaw, up);
    Bid128 objectUp = x.nextUp(upObject);
    check(objectUp.highBits() == up[0] && objectUp.lowBits() == up[1], "nextUp128");
    check(upObject.bits() == upRaw.bits(), "nextUp128Flags");
    StatusFlags downObject = new StatusFlags();
    StatusFlags downRaw = new StatusFlags();
    long[] down = new long[2];
    Bid128Raw.nextDown(x.highBits(), x.lowBits(), downRaw, down);
    Bid128 objectDown = x.nextDown(downObject);
    check(
        objectDown.highBits() == down[0] && objectDown.lowBits() == down[1],
        "nextDown128");
    check(downObject.bits() == downRaw.bits(), "nextDown128Flags");
  }

  private static void checkRound64(Bid64 x, RoundingMode mode) {
    StatusFlags objectFlags = new StatusFlags();
    StatusFlags rawFlags = new StatusFlags();
    long objectBits = x.roundIntegral(mode, false, objectFlags).toRawBits();
    long rawBits = Bid64Raw.roundIntegral(x.toRawBits(), mode, rawFlags, false);
    check(objectBits == rawBits, "round64Bits");
    check(objectFlags.bits() == rawFlags.bits(), "round64Flags");
    StatusFlags nearbyObject = new StatusFlags();
    StatusFlags nearbyRaw = new StatusFlags();
    check(
        x.nearbyInt(mode, nearbyObject).toRawBits()
            == Bid64Raw.nearbyint(x.toRawBits(), mode, nearbyRaw),
        "nearby64Bits");
    check(nearbyObject.bits() == nearbyRaw.bits(), "nearby64Flags");
  }

  private static void checkRound128(Bid128 x, RoundingMode mode) {
    StatusFlags objectFlags = new StatusFlags();
    StatusFlags rawFlags = new StatusFlags();
    Bid128 objectResult = x.roundIntegral(mode, false, objectFlags);
    long[] rawBits = new long[2];
    Bid128Raw.roundIntegral(
        x.highBits(), x.lowBits(), mode, rawFlags, false, rawBits);
    check(
        objectResult.highBits() == rawBits[0]
            && objectResult.lowBits() == rawBits[1],
        "round128Bits");
    check(objectFlags.bits() == rawFlags.bits(), "round128Flags");
  }

  private static void checkUnary64(String name, Bid64 x, RoundingMode mode) {
    try {
      Method object = Bid64.class.getMethod(
          name, RoundingMode.class, StatusFlags.class);
      Method raw = Bid64Raw.class.getMethod(
          name, long.class, RoundingMode.class, StatusFlags.class);
      StatusFlags objectFlags = new StatusFlags();
      StatusFlags rawFlags = new StatusFlags();
      Bid64 objectResult = (Bid64) object.invoke(x, mode, objectFlags);
      long rawResult = (Long) raw.invoke(null, x.toRawBits(), mode, rawFlags);
      check(
          objectResult.toRawBits() == rawResult,
          "unary64Bits:" + name + ":" + x);
      check(
          objectFlags.bits() == rawFlags.bits(),
          "unary64Flags:" + name + ":" + x);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("unary64:" + name, e);
    }
  }

  private static void checkUnary128(String name, Bid128 x, RoundingMode mode) {
    try {
      Method object = Bid128.class.getMethod(
          name, RoundingMode.class, StatusFlags.class);
      Method raw = Bid128Raw.class.getMethod(
          name,
          long.class,
          long.class,
          RoundingMode.class,
          StatusFlags.class,
          long[].class);
      StatusFlags objectFlags = new StatusFlags();
      StatusFlags rawFlags = new StatusFlags();
      Bid128 objectResult = (Bid128) object.invoke(x, mode, objectFlags);
      long[] rawBits = new long[2];
      raw.invoke(null, x.highBits(), x.lowBits(), mode, rawFlags, rawBits);
      check(
          objectResult.highBits() == rawBits[0]
              && objectResult.lowBits() == rawBits[1],
          "unary128Bits:" + name + ":" + x);
      check(
          objectFlags.bits() == rawFlags.bits(),
          "unary128Flags:" + name + ":" + x);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("unary128:" + name, e);
    }
  }

  private static void checkBinary64(
      String name, Bid64 left, Bid64 right, RoundingMode mode) {
    try {
      Method object = Bid64.class.getMethod(
          name, Bid64.class, RoundingMode.class, StatusFlags.class);
      Method raw = Bid64Raw.class.getMethod(
          name,
          long.class,
          long.class,
          RoundingMode.class,
          StatusFlags.class);
      StatusFlags objectFlags = new StatusFlags();
      StatusFlags rawFlags = new StatusFlags();
      Bid64 objectResult = (Bid64) object.invoke(left, right, mode, objectFlags);
      long rawResult = (Long) raw.invoke(
          null, left.toRawBits(), right.toRawBits(), mode, rawFlags);
      check(
          objectResult.toRawBits() == rawResult,
          "binary64Bits:" + name + ":" + left + "," + right);
      check(
          objectFlags.bits() == rawFlags.bits(),
          "binary64Flags:" + name + ":" + left + "," + right);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("binary64:" + name, e);
    }
  }

  private static void checkBinary128(
      String name, Bid128 left, Bid128 right, RoundingMode mode) {
    try {
      Method object = Bid128.class.getMethod(
          name, Bid128.class, RoundingMode.class, StatusFlags.class);
      Method raw = Bid128Raw.class.getMethod(
          name,
          long.class,
          long.class,
          long.class,
          long.class,
          RoundingMode.class,
          StatusFlags.class,
          long[].class);
      StatusFlags objectFlags = new StatusFlags();
      StatusFlags rawFlags = new StatusFlags();
      Bid128 objectResult =
          (Bid128) object.invoke(left, right, mode, objectFlags);
      long[] rawBits = new long[2];
      raw.invoke(
          null,
          left.highBits(),
          left.lowBits(),
          right.highBits(),
          right.lowBits(),
          mode,
          rawFlags,
          rawBits);
      check(
          objectResult.highBits() == rawBits[0]
              && objectResult.lowBits() == rawBits[1],
          "binary128Bits:" + name + ":" + left + "," + right);
      check(
          objectFlags.bits() == rawFlags.bits(),
          "binary128Flags:" + name + ":" + left + "," + right);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("binary128:" + name, e);
    }
  }

  private static void testCompatStatus() {
    long one64 = Bid64.parseExact("1").toRawBits();
    long zero64 = Bid64.POSITIVE_ZERO.toRawBits();
    int[] flags64 = {0};
    DecFloat16Compat.bid64Div(
        one64, zero64, RoundingMode.TIES_TO_EVEN.toIntel(), flags64);
    check((flags64[0] & StatusFlags.DIVIDE_BY_ZERO) != 0, "compatFlags64");

    Bid128 one128 = Bid128.parseExact("1");
    Bid128 zero128 = Bid128.POSITIVE_ZERO;
    long[] result128 = new long[2];
    int[] flags128 = {0};
    DecFloat34Compat.bid128Div(
        one128.highBits(), one128.lowBits(), zero128.highBits(), zero128.lowBits(),
        RoundingMode.TIES_TO_EVEN.toIntel(), result128, flags128);
    check((flags128[0] & StatusFlags.DIVIDE_BY_ZERO) != 0, "compatFlags128");
  }

  private static Bid128 rawFmod128(Bid128 x, Bid128 y) {
    long[] result = new long[2];
    Bid128Raw.fmod(
        x.highBits(), x.lowBits(), y.highBits(), y.lowBits(), new StatusFlags(), result);
    return Bid128.fromRawBits(result[0], result[1]);
  }

  private static Bid128 rawNextAfter128(Bid128 x, Bid128 y) {
    long[] result = new long[2];
    Bid128Raw.nextAfter(
        x.highBits(), x.lowBits(), y.highBits(), y.lowBits(), new StatusFlags(), result);
    return Bid128.fromRawBits(result[0], result[1]);
  }

  private static Bid128 rawQuantum128(Bid128 x) {
    long[] result = new long[2];
    Bid128Raw.quantum(x.highBits(), x.lowBits(), result);
    return Bid128.fromRawBits(result[0], result[1]);
  }

  private static Bid128 rawLogb128(Bid128 x) {
    long[] result = new long[2];
    Bid128Raw.logb(x.highBits(), x.lowBits(), new StatusFlags(), result);
    return Bid128.fromRawBits(result[0], result[1]);
  }

  private static void check(boolean condition, String name) {
    if (!condition) {
      throw new AssertionError(name);
    }
  }
}
