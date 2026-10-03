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

/**
 * Spark-facing decimal64 adapters over the LibBID raw API.
 *
 * <p>Operations that can raise IEEE status require a {@code statusOut}
 * argument.
 */
public final class DecFloat16Compat {
  private DecFloat16Compat() {
  }

  public static long bid64FromString(String s, int rounding, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    long result = Bid64Raw.fromString(s, RoundingMode.fromIntel(rounding), flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static String bid64ToString(long payload) {
    return Bid64Raw.toString(payload);
  }

  /** Spark SQL ordering: signed zeros equal and all NaNs equal and greatest. */
  public static int bid64Compare(long a, long b) {
    return DecFloatAdapters.sqlCompare64(a, b);
  }

  /** Spark SQL equality, which differs from IEEE encoding equality. */
  public static boolean bid64Equals(long a, long b) {
    return DecFloatAdapters.sqlEquals64(a, b);
  }

  public static boolean bid64IsNaN(long payload) {
    return Bid64Raw.isNaN(payload);
  }

  public static boolean bid64IsZero(long payload) {
    return Bid64Raw.isZero(payload);
  }

  public static long bid64Add(long a, long b, int rounding, int[] statusOut) {
    return Bid64Raw.add(a, b, rounding, statusOut);
  }

  public static long bid64Sub(long a, long b, int rounding, int[] statusOut) {
    return Bid64Raw.sub(a, b, rounding, statusOut);
  }

  public static long bid64Mul(long a, long b, int rounding, int[] statusOut) {
    return Bid64Raw.mul(a, b, rounding, statusOut);
  }

  public static long bid64Div(long a, long b, int rounding, int[] statusOut) {
    return Bid64Raw.div(a, b, rounding, statusOut);
  }

  public static long bid64Negate(long payload) {
    return Bid64Raw.negate(payload);
  }

  public static long bid64RoundIntegralZero(long payload, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    long result = Bid64Raw.roundIntegralZero(payload, flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static long bid64Abs(long payload) {
    return Bid64Raw.abs(payload);
  }

  public static long bid64Sign(long payload) {
    return DecFloatAdapters.sign64(payload);
  }

  public static long bid64Floor(long payload, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    long result = Bid64Raw.floor(payload, flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static long bid64Ceil(long payload, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    long result = Bid64Raw.ceil(payload, flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static long bid64RoundToScale(
      long payload, long targetExponent, int rounding, int[] statusOut) {
    return DecFloatAdapters.roundToScale64(
        payload, targetExponent, rounding, statusOut);
  }

  public static long bid64Sqrt(long payload, int rounding, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    long result = Bid64Raw.sqrt(payload, RoundingMode.fromIntel(rounding), flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static boolean bid64IsInf(long payload) {
    return Bid64Raw.isInf(payload);
  }

  public static long bid64FromInt64(long v, int rounding, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    long result = Bid64Raw.fromInt64(v, RoundingMode.fromIntel(rounding), flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static long bid64ToInt64(long payload, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    long result = Bid64Raw.toInt64Int(payload, flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static long binary64ToBid64(double v, int rounding, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    long result = Bid64Raw.fromBinary64(v, RoundingMode.fromIntel(rounding), flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static float bid64ToBinary32(long payload, int rounding, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    float result = Bid64Raw.toBinary32(payload, RoundingMode.fromIntel(rounding), flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static double bid64ToBinary64(long payload, int rounding, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    double result = Bid64Raw.toBinary64(payload, RoundingMode.fromIntel(rounding), flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static void bid64ToBid128(long payload, long[] payloadOut, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    Bid64Raw.toBid128(payload, payloadOut, flags);
    flags.copyTo(statusOut);
  }

  public static long bid64FromDecimal(
      long unscaledHi, long unscaledLo, int scale, int rounding, int[] statusOut) {
    return DecFloatAdapters.fromDecimal64(unscaledHi, unscaledLo, scale, rounding, statusOut);
  }

  public static int bid64ToDecimal(
      long payload,
      int targetPrecision,
      int targetScale,
      int rounding,
      long[] unscaledOut,
      int[] statusOut) {
    return DecFloatAdapters.toDecimal64(
        payload,
        targetPrecision,
        targetScale,
        rounding,
        unscaledOut,
        statusOut);
  }

  public static long bid64Canonicalize(long payload) {
    return DecFloatAdapters.canonicalize64(payload);
  }
}
