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
 * Signature-compatible replacements for {@code DecFloat34} JNI natives.
 *
 * <p>The original JNI signatures retain their flag-discarding behavior. Overloads with
 * {@code statusOut} expose IEEE status flags to Java callers.
 */
public final class DecFloat34Compat {
  private DecFloat34Compat() {
  }

  public static void bid128FromString(
      String s, int rounding, long[] payloadOut, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    Bid128Raw.fromString(s, RoundingMode.fromIntel(rounding), flags, payloadOut);
    flags.copyTo(statusOut);
  }

  public static String bid128ToString(long hi, long lo) {
    return Bid128Raw.toString(hi, lo);
  }

  public static int bid128Compare(long aHi, long aLo, long bHi, long bLo) {
    return DecFloatAdapters.compare128(aHi, aLo, bHi, bLo);
  }

  public static boolean bid128Equals(long aHi, long aLo, long bHi, long bLo) {
    return DecFloatAdapters.equals128(aHi, aLo, bHi, bLo);
  }

  public static boolean bid128IsNaN(long hi, long lo) {
    return Bid128Raw.isNaN(hi, lo);
  }

  public static boolean bid128IsZero(long hi, long lo) {
    return Bid128Raw.isZero(hi, lo);
  }

  public static void bid128Add(
      long aHi, long aLo, long bHi, long bLo, int rounding, long[] payloadOut) {
    bid128Add(aHi, aLo, bHi, bLo, rounding, payloadOut, null);
  }

  public static void bid128Add(
      long aHi, long aLo, long bHi, long bLo, int rounding, long[] payloadOut,
      int[] statusOut) {
    Bid128Raw.add(aHi, aLo, bHi, bLo, rounding, payloadOut, statusOut);
  }

  public static void bid128Sub(
      long aHi, long aLo, long bHi, long bLo, int rounding, long[] payloadOut) {
    bid128Sub(aHi, aLo, bHi, bLo, rounding, payloadOut, null);
  }

  public static void bid128Sub(
      long aHi, long aLo, long bHi, long bLo, int rounding, long[] payloadOut,
      int[] statusOut) {
    Bid128Raw.sub(aHi, aLo, bHi, bLo, rounding, payloadOut, statusOut);
  }

  public static void bid128Mul(
      long aHi, long aLo, long bHi, long bLo, int rounding, long[] payloadOut) {
    bid128Mul(aHi, aLo, bHi, bLo, rounding, payloadOut, null);
  }

  public static void bid128Mul(
      long aHi, long aLo, long bHi, long bLo, int rounding, long[] payloadOut,
      int[] statusOut) {
    Bid128Raw.mul(aHi, aLo, bHi, bLo, rounding, payloadOut, statusOut);
  }

  public static void bid128Div(
      long aHi, long aLo, long bHi, long bLo, int rounding, long[] payloadOut) {
    bid128Div(aHi, aLo, bHi, bLo, rounding, payloadOut, null);
  }

  public static void bid128Div(
      long aHi, long aLo, long bHi, long bLo, int rounding, long[] payloadOut,
      int[] statusOut) {
    Bid128Raw.div(aHi, aLo, bHi, bLo, rounding, payloadOut, statusOut);
  }

  public static void bid128Negate(long hi, long lo, long[] payloadOut) {
    Bid128Raw.negate(hi, lo, payloadOut);
  }

  public static void bid128RoundIntegralZero(long hi, long lo, long[] payloadOut) {
    bid128RoundIntegralZero(hi, lo, payloadOut, null);
  }

  public static void bid128RoundIntegralZero(
      long hi, long lo, long[] payloadOut, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    Bid128Raw.roundIntegralZero(hi, lo, flags, payloadOut);
    flags.copyTo(statusOut);
  }

  public static void bid128Abs(long hi, long lo, long[] payloadOut) {
    Bid128Raw.abs(hi, lo, payloadOut);
  }

  public static void bid128Sign(long hi, long lo, long[] payloadOut) {
    DecFloatAdapters.sign128(hi, lo, payloadOut);
  }

  public static void bid128Floor(long hi, long lo, long[] payloadOut) {
    bid128Floor(hi, lo, payloadOut, null);
  }

  public static void bid128Floor(
      long hi, long lo, long[] payloadOut, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    Bid128Raw.floor(hi, lo, flags, payloadOut);
    flags.copyTo(statusOut);
  }

  public static void bid128Ceil(long hi, long lo, long[] payloadOut) {
    bid128Ceil(hi, lo, payloadOut, null);
  }

  public static void bid128Ceil(
      long hi, long lo, long[] payloadOut, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    Bid128Raw.ceil(hi, lo, flags, payloadOut);
    flags.copyTo(statusOut);
  }

  public static void bid128RoundToScale(
      long hi, long lo, long targetExponent, int rounding, long[] payloadOut) {
    DecFloatAdapters.roundToScale128(hi, lo, targetExponent, rounding, payloadOut);
  }

  public static void bid128Sqrt(long hi, long lo, int rounding, long[] payloadOut) {
    bid128Sqrt(hi, lo, rounding, payloadOut, null);
  }

  public static void bid128Sqrt(
      long hi, long lo, int rounding, long[] payloadOut, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    Bid128Raw.sqrt(hi, lo, RoundingMode.fromIntel(rounding), flags, payloadOut);
    flags.copyTo(statusOut);
  }

  public static boolean bid128IsInf(long hi, long lo) {
    return Bid128Raw.isInf(hi, lo);
  }

  public static void bid128FromInt64(
      long v, int rounding, long[] payloadOut, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    Bid128Raw.fromInt64(v, RoundingMode.fromIntel(rounding), flags, payloadOut);
    flags.copyTo(statusOut);
  }

  public static long bid128ToInt64(long hi, long lo, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    long result = Bid128Raw.toInt64(hi, lo, RoundingMode.TOWARD_ZERO, flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static void binary64ToBid128(
      double v, int rounding, long[] payloadOut, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    Bid128Raw.fromBinary64(v, RoundingMode.fromIntel(rounding), flags, payloadOut);
    flags.copyTo(statusOut);
  }

  public static float bid128ToBinary32(long hi, long lo, int rounding, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    float result = Bid128Raw.toBinary32(hi, lo, RoundingMode.fromIntel(rounding), flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static double bid128ToBinary64(long hi, long lo, int rounding, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    double result = Bid128Raw.toBinary64(hi, lo, RoundingMode.fromIntel(rounding), flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static long bid128ToBid64(long hi, long lo, int rounding, int[] statusOut) {
    StatusFlags flags = new StatusFlags();
    long result = Bid128Raw.toBid64(hi, lo, RoundingMode.fromIntel(rounding), flags);
    flags.copyTo(statusOut);
    return result;
  }

  public static void bid128FromDecimal(
      long unscaledHi, long unscaledLo, int scale, int rounding, long[] payloadOut,
      int[] statusOut) {
    DecFloatAdapters.fromDecimal128(
        unscaledHi, unscaledLo, scale, rounding, payloadOut, statusOut);
  }

  public static int bid128ToDecimal(long hi, long lo, long[] unscaledOut, int[] statusOut) {
    return DecFloatAdapters.toDecimal128(hi, lo, unscaledOut, statusOut);
  }

  public static void bid128Canonicalize(long hi, long lo, long[] payloadOut) {
    DecFloatAdapters.canonicalize128(hi, lo, payloadOut);
  }
}
