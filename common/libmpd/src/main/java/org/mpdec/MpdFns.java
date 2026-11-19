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
package org.mpdec;

/**
 * Additional quiet decimal operations beyond core + - * / sqrt fma compare:
 * unary, min/max, integer rounding, quantize/reduce, remainder, scaleb/logb.
 */
public final class MpdFns {
  private MpdFns() {
  }

  /** True if the value is an integer (finite and no non-zero fraction). */
  public static boolean isIntegral(Mpd a) {
    if (a.isSpecial()) {
      return false;
    }
    if (a.isZero() || a.exp >= 0) {
      return true;
    }
    // Integral iff the -exp low digits of the coefficient are all zero.
    MpdLimbs.RoundResult rr = MpdLimbs.divPow10Round(
        a.data, a.len, -a.exp, MpdOps.scratch().a);
    return rr.roundDigit == 0 && rr.sticky == 0;
  }

  // ------------------------------------------------------------------ unary
  public static void abs(Mpd result, Mpd a, MpdContext ctx) {
    MpdOps.Scratch s = MpdOps.scratch();
    if (a.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isNan()) {
      result.setNan(false);
      return;
    }
    result.copyFrom(a);
    result.flags &= ~Mpd.SIGN_MASK;
    MpdOps.finish(result, ctx, s);
  }

  public static void plus(Mpd result, Mpd a, MpdContext ctx) {
    MpdOps.Scratch s = MpdOps.scratch();
    if (a.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isNan()) {
      result.setNan(false);
      return;
    }
    result.copyFrom(a);
    MpdOps.finish(result, ctx, s);
  }

  /** Unary minus (negate). */
  public static void minus(Mpd result, Mpd a, MpdContext ctx) {
    MpdOps.Scratch s = MpdOps.scratch();
    if (a.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isNan()) {
      result.setNan(false);
      return;
    }
    result.copyFrom(a);
    result.flags ^= Mpd.SIGN_MASK;
    MpdOps.finish(result, ctx, s);
  }

  // --------------------------------------------------------------- min / max
  public static void max(Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    minMax(result, a, b, ctx, true, false);
  }

  public static void min(Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    minMax(result, a, b, ctx, false, false);
  }

  public static void maxMag(Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    minMax(result, a, b, ctx, true, true);
  }

  public static void minMag(Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    minMax(result, a, b, ctx, false, true);
  }

  private static void minMax(
      Mpd result, Mpd a, Mpd b, MpdContext ctx,
      boolean wantMax, boolean byMag) {
    MpdOps.Scratch s = MpdOps.scratch();
    if (a.isSnan() || b.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    // IEEE decimal maxNum/minNum: if one is qNaN, return the other.
    if (a.isNan()) {
      result.copyFrom(b);
      MpdOps.finish(result, ctx, s);
      return;
    }
    if (b.isNan()) {
      result.copyFrom(a);
      MpdOps.finish(result, ctx, s);
      return;
    }
    int cmp;
    if (byMag) {
      cmp = cmpAbsOnly(a, b);
    } else {
      cmp = MpdOps.compare(a, b);
    }
    Mpd choose;
    if (cmp == 0) {
      // Prefer +0 over -0 for max; -0 over +0 for min when equal.
      if (wantMax) {
        choose = a.isNegative() ? b : a;
      } else {
        choose = a.isNegative() ? a : b;
      }
    } else if (wantMax) {
      choose = cmp > 0 ? a : b;
    } else {
      choose = cmp < 0 ? a : b;
    }
    result.copyFrom(choose);
    MpdOps.finish(result, ctx, s);
  }

  private static int cmpAbsOnly(Mpd a, Mpd b) {
    if (a.isInfinite() || b.isInfinite()) {
      if (a.isInfinite() && b.isInfinite()) {
        return 0;
      }
      return a.isInfinite() ? 1 : -1;
    }
    if (a.isZero() && b.isZero()) {
      return 0;
    }
    if (a.isZero()) {
      return -1;
    }
    if (b.isZero()) {
      return 1;
    }
    int ae = a.adjExp();
    int be = b.adjExp();
    if (ae != be) {
      return ae < be ? -1 : 1;
    }
    if (a.digits == b.digits) {
      return MpdLimbs.cmpAbs(a.data, a.len, b.data, b.len);
    }
    MpdOps.Scratch s = MpdOps.scratch();
    if (a.digits > b.digits) {
      int bl = MpdLimbs.mulPow10(b.data, b.len, a.digits - b.digits, s.b);
      return MpdLimbs.cmpAbs(a.data, a.len, s.b, bl);
    }
    int al = MpdLimbs.mulPow10(a.data, a.len, b.digits - a.digits, s.a);
    return MpdLimbs.cmpAbs(s.a, al, b.data, b.len);
  }

  // ------------------------------------------------------- integer rounding
  public static void trunc(Mpd result, Mpd a, MpdContext ctx) {
    roundToInt(result, a, ctx, MpdRound.DOWN);
  }

  public static void floor(Mpd result, Mpd a, MpdContext ctx) {
    roundToInt(result, a, ctx, MpdRound.FLOOR);
  }

  public static void ceil(Mpd result, Mpd a, MpdContext ctx) {
    roundToInt(result, a, ctx, MpdRound.CEILING);
  }

  public static void roundToInt(Mpd result, Mpd a, MpdContext ctx) {
    roundToInt(result, a, ctx, ctx.round);
  }

  private static void roundToInt(
      Mpd result, Mpd a, MpdContext ctx, MpdRound mode) {
    MpdOps.Scratch s = MpdOps.scratch();
    if (a.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isNan()) {
      result.setNan(false);
      return;
    }
    if (a.isInfinite()) {
      result.copyFrom(a);
      return;
    }
    if (a.isZero() || a.exp >= 0) {
      result.copyFrom(a);
      MpdOps.finish(result, ctx, s);
      return;
    }
    int drop = -a.exp;
    MpdLimbs.RoundResult rr = MpdLimbs.divPow10Round(
        a.data, a.len, drop, s.a);
    boolean bump = shouldBump(
        a.isNegative(), rr.roundDigit, rr.sticky != 0, mode, rr.len, s.a);
    result.flags = a.flags & Mpd.SIGN_MASK;
    result.exp = 0;
    result.len = rr.len;
    System.arraycopy(s.a, 0, result.data, 0, rr.len);
    if (rr.roundDigit != 0 || rr.sticky != 0) {
      ctx.status.set(MpdStatus.INEXACT | MpdStatus.ROUNDED);
    }
    if (bump) {
      int len = MpdLimbs.addAbs(result.data, result.len, ONE, 1, s.b);
      System.arraycopy(s.b, 0, result.data, 0, len);
      result.len = len;
    }
    if (MpdLimbs.digitsOf(result.data, result.len) == 0) {
      result.setZero(a.isNegative());
      result.exp = 0;
    }
    MpdOps.finish(result, ctx, s);
  }

  private static boolean shouldBump(
      boolean negative, int roundDigit, boolean sticky,
      MpdRound mode, int len, long[] coeff) {
    return switch (mode) {
      case DOWN -> false;
      case UP -> roundDigit != 0 || sticky;
      case FLOOR -> negative && (roundDigit != 0 || sticky);
      case CEILING -> !negative && (roundDigit != 0 || sticky);
      case HALF_UP -> roundDigit >= 5;
      case HALF_DOWN -> roundDigit > 5 || (roundDigit == 5 && sticky);
      case HALF_EVEN -> {
        if (roundDigit > 5 || (roundDigit == 5 && sticky)) {
          yield true;
        }
        if (roundDigit < 5) {
          yield false;
        }
        yield len > 0 && (coeff[0] & 1L) != 0L;
      }
    };
  }

  // ------------------------------------------------- quantize / same / reduce
  public static boolean sameQuantum(Mpd a, Mpd b) {
    if (a.isSpecial() || b.isSpecial()) {
      return a.isInfinite() && b.isInfinite();
    }
    return a.exp == b.exp;
  }

  public static void quantize(Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    MpdOps.Scratch s = MpdOps.scratch();
    if (a.isSnan() || b.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isNan() || b.isNan()) {
      result.setNan(false);
      return;
    }
    if (a.isInfinite() || b.isInfinite()) {
      if (a.isInfinite() && b.isInfinite()) {
        result.copyFrom(a);
        return;
      }
      result.setNan(false);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    int target = b.exp;
    result.copyFrom(a);
    if (a.isZero()) {
      result.setZero(a.isNegative());
      result.exp = target;
      MpdOps.finish(result, ctx, s);
      return;
    }
    int diff = target - a.exp;
    if (diff == 0) {
      MpdOps.finish(result, ctx, s);
      // After finish exp may change under clamp; check digit fit vs target.
      return;
    }
    if (diff < 0) {
      int pad = -diff;
      if (a.digits + pad > ctx.prec) {
        result.setNan(false);
        ctx.status.set(MpdStatus.INVALID_OPERATION);
        return;
      }
      int len = MpdLimbs.mulPow10(a.data, a.len, pad, s.a);
      result.flags = a.flags & Mpd.SIGN_MASK;
      result.exp = target;
      result.len = len;
      System.arraycopy(s.a, 0, result.data, 0, len);
      MpdOps.finish(result, ctx, s);
      return;
    }
    // diff > 0: drop digits with rounding
    MpdLimbs.RoundResult rr = MpdLimbs.divPow10Round(
        a.data, a.len, diff, s.a);
    boolean bump = shouldBump(
        a.isNegative(), rr.roundDigit, rr.sticky != 0,
        ctx.round, rr.len, s.a);
    result.flags = a.flags & Mpd.SIGN_MASK;
    result.exp = target;
    result.len = rr.len;
    System.arraycopy(s.a, 0, result.data, 0, rr.len);
    if (rr.roundDigit != 0 || rr.sticky != 0) {
      ctx.status.set(MpdStatus.INEXACT | MpdStatus.ROUNDED);
    }
    if (bump) {
      int len = MpdLimbs.addAbs(result.data, result.len, ONE, 1, s.b);
      System.arraycopy(s.b, 0, result.data, 0, len);
      result.len = len;
    }
    int digs = MpdLimbs.digitsOf(result.data, result.len);
    if (digs > ctx.prec) {
      result.setNan(false);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (digs == 0) {
      result.setZero(a.isNegative());
      result.exp = target;
    }
    MpdOps.finish(result, ctx, s);
  }

  /** Strip trailing zeros from the coefficient (shortest cohort). */
  public static void reduce(Mpd result, Mpd a, MpdContext ctx) {
    MpdOps.Scratch s = MpdOps.scratch();
    if (a.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isNan()) {
      result.setNan(false);
      return;
    }
    if (a.isInfinite() || a.isZero()) {
      result.copyFrom(a);
      return;
    }
    result.copyFrom(a);
    while (result.digits > 1) {
      // Trailing decimal digit zero?
      if (result.exp >= ctx.emax) {
        break;
      }
      if ((result.data[0] % 10L) != 0L) {
        break;
      }
      MpdLimbs.Result rr = MpdLimbs.divPow10(
          result.data, result.len, 1, s.a);
      System.arraycopy(s.a, 0, result.data, 0, rr.len);
      result.len = rr.len;
      result.exp += 1;
      result.digits = MpdLimbs.digitsOf(result.data, result.len);
    }
    MpdOps.finish(result, ctx, s);
  }

  // ----------------------------------------------------------- remainder
  public static void divint(Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    MpdOps.Scratch s = MpdOps.scratch();
    if (specialDivCheck(result, a, b, ctx)) {
      return;
    }
    MpdRound saved = ctx.round;
    ctx.round = MpdRound.DOWN;
    MpdOps.div(s.tmp, a, b, ctx);
    ctx.round = saved;
    trunc(result, s.tmp, ctx);
  }

  public static void rem(Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    MpdOps.Scratch s = MpdOps.scratch();
    if (specialDivCheck(result, a, b, ctx)) {
      return;
    }
    divint(s.tmp, a, b, ctx);
    MpdOps.mul(s.tmp2, b, s.tmp, ctx);
    MpdOps.sub(result, a, s.tmp2, ctx);
  }

  /**
   * Remainder with nearest/even quotient (IEEE-style remainder).
   */
  public static void remNear(Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    MpdOps.Scratch s = MpdOps.scratch();
    if (specialDivCheck(result, a, b, ctx)) {
      return;
    }
    MpdRound saved = ctx.round;
    ctx.round = MpdRound.HALF_EVEN;
    MpdOps.div(s.tmp, a, b, ctx);
    ctx.round = saved;
    roundToInt(s.tmp2, s.tmp, ctx);
    MpdOps.mul(s.tmp3, b, s.tmp2, ctx);
    MpdOps.sub(result, a, s.tmp3, ctx);
  }

  /** Integer quotient and remainder; rem has the sign of the dividend. */
  public static void divmod(
      Mpd quot, Mpd remOut, Mpd a, Mpd b, MpdContext ctx) {
    if (specialDivCheck(quot, a, b, ctx)) {
      remOut.setNan(false);
      return;
    }
    divint(quot, a, b, ctx);
    MpdOps.Scratch s = MpdOps.scratch();
    MpdOps.mul(s.tmp, b, quot, ctx);
    MpdOps.sub(remOut, a, s.tmp, ctx);
  }

  private static boolean specialDivCheck(
      Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    if (a.isSnan() || b.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return true;
    }
    if (a.isNan() || b.isNan()) {
      result.setNan(false);
      return true;
    }
    if (b.isZero()) {
      result.setNan(false);
      ctx.status.set(
          MpdStatus.DIVISION_UNDEFINED | MpdStatus.INVALID_OPERATION);
      return true;
    }
    if (a.isInfinite()) {
      result.setNan(false);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return true;
    }
    return false;
  }

  // ---------------------------------------------------------- scaleb / logb
  /** result = a * 10^n (exact exponent adjust), n must be a finite integer. */
  public static void scaleb(Mpd result, Mpd a, Mpd n, MpdContext ctx) {
    MpdOps.Scratch s = MpdOps.scratch();
    if (a.isSnan() || n.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isNan() || n.isNan()) {
      result.setNan(false);
      return;
    }
    if (n.isInfinite() || n.exp < 0) {
      // require integer n
      result.setNan(false);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isInfinite()) {
      result.copyFrom(a);
      return;
    }
    long nVal = intValueExact(n);
    if (nVal == Long.MIN_VALUE) {
      result.setNan(false);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    result.copyFrom(a);
    long newExp = (long) result.exp + nVal;
    if (newExp > Integer.MAX_VALUE || newExp < Integer.MIN_VALUE) {
      result.setNan(false);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    result.exp = (int) newExp;
    MpdOps.finish(result, ctx, s);
  }

  /** Floor of log10(|a|) = adjusted exponent for finite nonzero a. */
  public static void logb(Mpd result, Mpd a, MpdContext ctx) {
    MpdOps.Scratch s = MpdOps.scratch();
    if (a.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isNan()) {
      result.setNan(false);
      return;
    }
    if (a.isInfinite()) {
      result.setInfinite(false);
      return;
    }
    if (a.isZero()) {
      result.setInfinite(true);
      ctx.status.set(MpdStatus.DIVISION_BY_ZERO);
      return;
    }
    int adj = a.adjExp();
    fromInt(result, adj);
    MpdOps.finish(result, ctx, s);
  }

  static void fromInt(Mpd out, long v) {
    out.clearFinite();
    if (v < 0) {
      out.flags = Mpd.SIGN_MASK;
      v = -v;
    }
    if (v == 0L) {
      out.setZero(false);
      return;
    }
    int len = 0;
    while (v > 0L) {
      out.data[len++] = v % Mpd.RADIX;
      v /= Mpd.RADIX;
    }
    out.len = len;
    out.exp = 0;
    out.digits = MpdLimbs.digitsOf(out.data, len);
  }

  /** Returns Long.MIN_VALUE if not a small exact integer. */
  static long intValueExact(Mpd n) {
    if (n.isSpecial() || n.exp < 0) {
      return Long.MIN_VALUE;
    }
    if (n.isZero()) {
      return 0L;
    }
    // value = coeff * 10^exp
    if (n.digits + n.exp > 18) {
      return Long.MIN_VALUE;
    }
    long coeff = 0L;
    for (int i = n.len - 1; i >= 0; i--) {
      coeff = coeff * Mpd.RADIX + n.data[i];
    }
    for (int i = 0; i < n.exp; i++) {
      if (coeff > Long.MAX_VALUE / 10L) {
        return Long.MIN_VALUE;
      }
      coeff *= 10L;
    }
    return n.isNegative() ? -coeff : coeff;
  }

  private static final long[] ONE = {1L};
}
