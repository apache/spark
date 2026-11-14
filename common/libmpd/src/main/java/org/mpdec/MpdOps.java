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
 * Quiet arithmetic on {@link Mpd} values. Results are written into the
 * caller-supplied {@code result} buffer. Temporary work buffers may be
 * reused through {@link Scratch}.
 */
public final class MpdOps {
  private MpdOps() {
  }

  /** Thread-confined scratch space for hot paths. */
  public static final class Scratch {
    final long[] a = new long[Mpd.WORK_LIMBS];
    final long[] b = new long[Mpd.WORK_LIMBS];
    final long[] c = new long[Mpd.WORK_LIMBS];
    final long[] d = new long[Mpd.WORK_LIMBS];
    final long[] uDiv = new long[Mpd.WORK_LIMBS];
    final long[] vDiv = new long[Mpd.WORK_LIMBS];
    final long[] pqDiv = new long[Mpd.WORK_LIMBS];
    final Mpd tmp = Mpd.work();
    final Mpd tmp2 = Mpd.work();
    final Mpd tmp3 = Mpd.work();
    /** Reused elevated-precision context for Newton (sqrt). */
    final MpdContext workCtx = new MpdContext(
        Mpd.MAX_DIGITS, 999999, -999999, MpdRound.HALF_EVEN, 0);
  }

  private static final ThreadLocal<Scratch> TL =
      ThreadLocal.withInitial(Scratch::new);

  static Scratch scratch() {
    return TL.get();
  }

  public static void add(Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    addSub(result, a, b, false, ctx, scratch());
  }

  public static void sub(Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    addSub(result, a, b, true, ctx, scratch());
  }

  public static void mul(Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    mul(result, a, b, ctx, scratch());
  }

  public static void div(Mpd result, Mpd a, Mpd b, MpdContext ctx) {
    div(result, a, b, ctx, scratch());
  }

  public static void sqrt(Mpd result, Mpd a, MpdContext ctx) {
    sqrt(result, a, ctx, scratch());
  }

  public static void fma(Mpd result, Mpd a, Mpd b, Mpd c, MpdContext ctx) {
    Scratch s = scratch();
    // exact mul into tmp (skip round), then add c, then one finalize.
    if (handleSpecialMul(result, a, b, ctx)) {
      return;
    }
    boolean neg = a.isNegative() ^ b.isNegative();
    int len = MpdLimbs.mulAbs(a.data, a.len, b.data, b.len, s.a);
    s.tmp.flags = neg ? Mpd.SIGN_MASK : 0;
    s.tmp.exp = a.exp + b.exp;
    s.tmp.len = len;
    System.arraycopy(s.a, 0, s.tmp.data, 0, len);
    s.tmp.digits = MpdLimbs.digitsOf(s.tmp.data, len);
    // skip finalize; add c with huge prec then finalize once
    int saved = ctx.prec;
    ctx.prec = Mpd.MAX_DIGITS;
    addSub(s.tmp2, s.tmp, c, false, ctx, s);
    ctx.prec = saved;
    result.copyFrom(s.tmp2);
    finish(result, ctx, s);
  }

  public static int compare(Mpd a, Mpd b) {
    if (a.isNan() || b.isNan()) {
      return -2; // unordered
    }
    if (a.isInfinite() || b.isInfinite()) {
      if (a.isInfinite() && b.isInfinite()) {
        if (a.isNegative() == b.isNegative()) {
          return 0;
        }
        return a.isNegative() ? -1 : 1;
      }
      if (a.isInfinite()) {
        return a.isNegative() ? -1 : 1;
      }
      return b.isNegative() ? 1 : -1;
    }
    if (a.isZero() && b.isZero()) {
      return 0;
    }
    if (a.isZero()) {
      return b.isNegative() ? 1 : -1;
    }
    if (b.isZero()) {
      return a.isNegative() ? -1 : 1;
    }
    if (a.isNegative() != b.isNegative()) {
      return a.isNegative() ? -1 : 1;
    }
    int cmp = cmpAbsMagnitude(a, b);
    return a.isNegative() ? -cmp : cmp;
  }

  private static int cmpAbsMagnitude(Mpd a, Mpd b) {
    int ae = a.adjExp();
    int be = b.adjExp();
    if (ae != be) {
      return ae < be ? -1 : 1;
    }
    int ad = a.digits;
    int bd = b.digits;
    if (ad == bd) {
      return MpdLimbs.cmpAbs(a.data, a.len, b.data, b.len);
    }
    // same adjusted exponent, different digit counts: align by shifting.
    Scratch s = scratch();
    if (ad > bd) {
      int bl = MpdLimbs.mulPow10(b.data, b.len, ad - bd, s.b);
      return MpdLimbs.cmpAbs(a.data, a.len, s.b, bl);
    }
    int al = MpdLimbs.mulPow10(a.data, a.len, bd - ad, s.a);
    return MpdLimbs.cmpAbs(s.a, al, b.data, b.len);
  }

  private static void addSub(
      Mpd result, Mpd a, Mpd b, boolean subtract, MpdContext ctx, Scratch s) {
    if (a.isSnan() || b.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isNan() || b.isNan()) {
      result.setNan(false);
      return;
    }
    boolean bNeg = b.isNegative() ^ subtract;
    if (a.isInfinite() || b.isInfinite()) {
      if (a.isInfinite() && (b.isInfinite()) && (a.isNegative() != bNeg)) {
        result.setNan(false);
        ctx.status.set(MpdStatus.INVALID_OPERATION);
        return;
      }
      if (a.isInfinite()) {
        result.setInfinite(a.isNegative());
      } else {
        result.setInfinite(bNeg);
      }
      return;
    }
    if (a.isZero() && !b.isZero()) {
      result.copyFrom(b);
      result.flags = bNeg ? Mpd.SIGN_MASK : 0;
      finish(result, ctx, s);
      return;
    }
    if (b.isZero()) {
      result.copyFrom(a);
      finish(result, ctx, s);
      return;
    }

    // Align to the smaller exponent.
    int exp = Math.min(a.exp, b.exp);
    int aShift = a.exp - exp;
    int bShift = b.exp - exp;
    int aLen = MpdLimbs.mulPow10(a.data, a.len, aShift, s.a);
    int bLen = MpdLimbs.mulPow10(b.data, b.len, bShift, s.b);
    boolean aNeg = a.isNegative();
    int cmp = MpdLimbs.cmpAbs(s.a, aLen, s.b, bLen);
    if (aNeg == bNeg) {
      int len = MpdLimbs.addAbs(s.a, aLen, s.b, bLen, s.c);
      result.flags = aNeg ? Mpd.SIGN_MASK : 0;
      result.exp = exp;
      result.len = len;
      System.arraycopy(s.c, 0, result.data, 0, len);
    } else if (cmp == 0) {
      result.setZero(false);
      result.exp = exp;
      finish(result, ctx, s);
      return;
    } else if (cmp > 0) {
      int len = MpdLimbs.subAbs(s.a, aLen, s.b, bLen, s.c);
      result.flags = aNeg ? Mpd.SIGN_MASK : 0;
      result.exp = exp;
      result.len = len;
      System.arraycopy(s.c, 0, result.data, 0, len);
    } else {
      int len = MpdLimbs.subAbs(s.b, bLen, s.a, aLen, s.c);
      result.flags = bNeg ? Mpd.SIGN_MASK : 0;
      result.exp = exp;
      result.len = len;
      System.arraycopy(s.c, 0, result.data, 0, len);
    }
    finish(result, ctx, s);
  }

  private static void mul(
      Mpd result, Mpd a, Mpd b, MpdContext ctx, Scratch s) {
    if (handleSpecialMul(result, a, b, ctx)) {
      return;
    }
    boolean neg = a.isNegative() ^ b.isNegative();
    if (a.isZero() || b.isZero()) {
      result.setZero(neg);
      result.exp = a.exp + b.exp;
      finish(result, ctx, s);
      return;
    }
    // Fast path: both single-limb, product fits in a long.
    if (a.len == 1 && b.len == 1) {
      long prod = a.data[0] * b.data[0];
      result.flags = neg ? Mpd.SIGN_MASK : 0;
      result.exp = a.exp + b.exp;
      result.data[0] = prod % Mpd.RADIX;
      long hi = prod / Mpd.RADIX;
      if (hi == 0L) {
        result.len = 1;
      } else {
        result.data[1] = hi;
        result.len = 2;
      }
      finish(result, ctx, s);
      return;
    }
    int len = MpdLimbs.mulAbs(a.data, a.len, b.data, b.len, s.a);
    result.flags = neg ? Mpd.SIGN_MASK : 0;
    result.exp = a.exp + b.exp;
    result.len = len;
    System.arraycopy(s.a, 0, result.data, 0, len);
    finish(result, ctx, s);
  }

  private static boolean handleSpecialMul(
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
    if (a.isInfinite() || b.isInfinite()) {
      if (a.isZero() || b.isZero()) {
        result.setNan(false);
        ctx.status.set(MpdStatus.INVALID_OPERATION);
        return true;
      }
      result.setInfinite(a.isNegative() ^ b.isNegative());
      return true;
    }
    return false;
  }

  private static void div(
      Mpd result, Mpd a, Mpd b, MpdContext ctx, Scratch s) {
    if (a.isSnan() || b.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isNan() || b.isNan()) {
      result.setNan(false);
      return;
    }
    boolean neg = a.isNegative() ^ b.isNegative();
    if (b.isZero()) {
      if (a.isZero()) {
        result.setNan(false);
        ctx.status.set(MpdStatus.DIVISION_UNDEFINED | MpdStatus.INVALID_OPERATION);
      } else {
        result.setInfinite(neg);
        ctx.status.set(MpdStatus.DIVISION_BY_ZERO);
      }
      return;
    }
    if (a.isInfinite()) {
      if (b.isInfinite()) {
        result.setNan(false);
        ctx.status.set(MpdStatus.INVALID_OPERATION);
      } else {
        result.setInfinite(neg);
      }
      return;
    }
    if (b.isInfinite()) {
      result.setZero(neg);
      finish(result, ctx, s);
      return;
    }
    if (a.isZero()) {
      result.setZero(neg);
      result.exp = a.exp - b.exp;
      finish(result, ctx, s);
      return;
    }

    // Shift dividend left so quotient has prec+1 digits (guard).
    int shift = (ctx.prec + 1) - (a.digits - b.digits);
    if (shift < 0) {
      shift = 0;
    }
    int uLen = MpdLimbs.mulPow10(a.data, a.len, shift, s.a);
    int qLen = MpdLimbs.divAbs(
        s.a, uLen, b.data, b.len, s.c, s.d,
        s.uDiv, s.vDiv, s.pqDiv);
    result.flags = neg ? Mpd.SIGN_MASK : 0;
    result.exp = a.exp - b.exp - shift;
    result.len = qLen;
    System.arraycopy(s.c, 0, result.data, 0, qLen);
    // Remainder is sticky for rounding; keep the prec+1 guard digit as-is.
    finish(result, ctx, s, remainderNonZero(s.d, b.len));
  }

  private static void sqrt(Mpd result, Mpd a, MpdContext ctx, Scratch s) {
    if (a.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isNan()) {
      result.setNan(false);
      return;
    }
    if (a.isNegative()) {
      result.setNan(false);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (a.isInfinite()) {
      result.setInfinite(false);
      return;
    }
    if (a.isZero()) {
      // Prefer even exponent cohort for +0; keep sign for -0.
      result.setZero(a.isNegative());
      int e = a.exp;
      result.exp = e >= 0 ? (e / 2) * 2 : -(((-e + 1) / 2) * 2);
      finish(result, ctx, s);
      return;
    }

    // Newton: y' = (y + a/y)/2 at slightly elevated precision.
    int workPrec = Math.min(Mpd.MAX_DIGITS, ctx.prec + 3);
    MpdContext work = s.workCtx;
    work.prec = workPrec;
    work.emax = ctx.emax;
    work.emin = ctx.emin;
    work.round = ctx.round;
    work.clamp = ctx.clamp;
    work.status.clear();

    seedSqrt(s.tmp, a, s);

    // Double seed already has ~15 digits; each Newton roughly doubles digits.
    int iters = 2;
    if (workPrec > 20) {
      iters = 3;
    }
    if (workPrec > 40) {
      iters = 4;
    }
    if (workPrec > 80) {
      iters = 5;
    }
    for (int i = 0; i < iters; i++) {
      div(s.tmp2, a, s.tmp, work, s); // a/y
      addSub(s.tmp3, s.tmp, s.tmp2, false, work, s); // y + a/y
      halve(s.tmp, s.tmp3, work, s);
    }
    result.copyFrom(s.tmp);
    finish(result, ctx, s);
  }

  /**
   * Seed sqrt from a double approximation built from the leading limbs,
   * avoiding BigDecimal. Falls back to 10^(adjExp/2) when needed.
   */
  private static void seedSqrt(Mpd out, Mpd a, Scratch s) {
    int adj = a.adjExp();
    int halfAdj = adj >= 0 ? adj / 2 : -((-adj + 1) / 2);
    int drop = Math.max(0, a.digits - 15);
    MpdLimbs.Result rr = MpdLimbs.divPow10(a.data, a.len, drop, s.a);
    double coeff = 0.0;
    for (int i = rr.len - 1; i >= 0; i--) {
      coeff = coeff * (double) Mpd.RADIX + (double) s.a[i];
    }
    double dv = coeff * Math.pow(10.0, a.exp + drop);
    if (Double.isFinite(dv) && dv > 0.0) {
      double root = Math.sqrt(dv);
      if (Double.isFinite(root) && root > 0.0) {
        MpdFormat.parse(out, Double.toString(root));
        if (!out.isSpecial() && !out.isZero() && !out.isNegative()) {
          return;
        }
      }
    }
    MpdFormat.parse(out, "1E" + halfAdj);
  }

  /** result = value / 2 with context rounding. */
  private static void halve(
      Mpd result, Mpd value, MpdContext ctx, Scratch s) {
    if (value.isSpecial() || value.isZero()) {
      result.copyFrom(value);
      finish(result, ctx, s);
      return;
    }
    // Multiply by 5 and decrement exponent => /2.
    s.b[0] = 5L;
    int len = MpdLimbs.mulAbs(value.data, value.len, s.b, 1, s.a);
    result.flags = value.flags & Mpd.SIGN_MASK;
    result.exp = value.exp - 1;
    result.len = len;
    System.arraycopy(s.a, 0, result.data, 0, len);
    finish(result, ctx, s);
  }

  static void finish(Mpd result, MpdContext ctx, Scratch s) {
    finish(result, ctx, s, false);
  }

  static void finish(Mpd result, MpdContext ctx, Scratch s, boolean stickyIn) {
    if (result.isSpecial()) {
      return;
    }
    result.len = MpdLimbs.normalizeLen(result.data, result.len);
    result.digits = MpdLimbs.digitsOf(result.data, result.len);
    if (result.digits == 0) {
      result.setZero(result.isNegative());
      // clamp zero exponent if needed
      if (ctx.clamp != 0) {
        int etiny = ctx.emin - (ctx.prec - 1);
        if (result.exp < etiny) {
          result.exp = etiny;
          ctx.status.set(MpdStatus.CLAMPED);
        }
      }
      return;
    }

    // Round to precision.
    if (result.digits > ctx.prec) {
      int drop = result.digits - ctx.prec;
      MpdLimbs.RoundResult rr = MpdLimbs.divPow10Round(
          result.data, result.len, drop, s.a);
      int roundDigit = rr.roundDigit;
      int sticky = (rr.sticky != 0 || stickyIn) ? 1 : 0;

      boolean bump = shouldRoundUp(
          result.isNegative(), roundDigit, sticky != 0,
          (int) (s.a[0] & 1L), ctx.round, rr.len, s.a);

      result.exp += drop;
      result.len = rr.len;
      System.arraycopy(s.a, 0, result.data, 0, rr.len);
      result.digits = MpdLimbs.digitsOf(result.data, result.len);
      ctx.status.set(MpdStatus.ROUNDED);
      if (roundDigit != 0 || sticky != 0) {
        ctx.status.set(MpdStatus.INEXACT);
      }
      if (bump) {
        int len = MpdLimbs.addAbs(
            result.data, result.len, ONE, 1, s.b);
        System.arraycopy(s.b, 0, result.data, 0, len);
        result.len = len;
        result.digits = MpdLimbs.digitsOf(result.data, len);
        if (result.digits > ctx.prec) {
          // 999..9 -> 1000..0
          MpdLimbs.Result trim = MpdLimbs.divPow10(
              result.data, result.len, 1, s.a);
          System.arraycopy(s.a, 0, result.data, 0, trim.len);
          result.len = trim.len;
          result.exp += 1;
          result.digits = MpdLimbs.digitsOf(result.data, result.len);
        }
      }
    } else if (stickyIn) {
      // Exact digit count but non-zero remainder from division: round from sticky.
      ctx.status.set(MpdStatus.INEXACT | MpdStatus.ROUNDED);
      boolean bump = shouldRoundUp(
          result.isNegative(), 0, true,
          (int) (result.data[0] & 1L), ctx.round, result.len, result.data);
      if (bump) {
        int len = MpdLimbs.addAbs(result.data, result.len, ONE, 1, s.b);
        System.arraycopy(s.b, 0, result.data, 0, len);
        result.len = len;
        result.digits = MpdLimbs.digitsOf(result.data, len);
        if (result.digits > ctx.prec) {
          MpdLimbs.Result trim = MpdLimbs.divPow10(
              result.data, result.len, 1, s.a);
          System.arraycopy(s.a, 0, result.data, 0, trim.len);
          result.len = trim.len;
          result.exp += 1;
          result.digits = MpdLimbs.digitsOf(result.data, result.len);
        }
      }
    }

    // Fold trailing zero limbs into exponent? Keep preferred quantum: do not
    // strip trailing zeros (cohorts preserved).

    int adj = result.adjExp();
    int etiny = ctx.emin - (ctx.prec - 1);
    if (adj > ctx.emax) {
      // overflow
      ctx.status.set(
          MpdStatus.OVERFLOW | MpdStatus.INEXACT | MpdStatus.ROUNDED);
      if (roundOverflowToInf(result.isNegative(), ctx.round)) {
        result.setInfinite(result.isNegative());
      } else {
        // clamp to max finite
        setMaxFinite(result, ctx);
      }
      return;
    }
    if (adj < ctx.emin) {
      // subnormal / underflow path: shift so adj becomes emin-related
      ctx.status.set(MpdStatus.SUBNORMAL);
      int need = result.digits - (adj - etiny + 1);
      // target digits so that exp = etiny and digits fit
      if (result.exp < etiny) {
        int shift = etiny - result.exp;
        // shift right by `shift` digits toward etiny
        // equivalent: reduce coeff, increase exp
        if (shift > 0) {
          MpdLimbs.Result rr = MpdLimbs.divPow10(
              result.data, result.len, shift, s.a);
          int rd = leadingDiscardedDigit(result.data, result.len, shift);
          int st = discardedSticky(result.data, result.len, shift, rd);
          boolean bump = shouldRoundUp(
              result.isNegative(), rd, st != 0,
              (int) (s.a[0] & 1L), ctx.round, rr.len, s.a);
          result.exp = etiny;
          result.len = rr.len;
          System.arraycopy(s.a, 0, result.data, 0, rr.len);
          result.digits = MpdLimbs.digitsOf(result.data, result.len);
          ctx.status.set(MpdStatus.ROUNDED);
          if (rd != 0 || st != 0) {
            ctx.status.set(MpdStatus.INEXACT | MpdStatus.UNDERFLOW);
          }
          if (bump) {
            int len = MpdLimbs.addAbs(
                result.data, result.len, ONE, 1, s.b);
            System.arraycopy(s.b, 0, result.data, 0, len);
            result.len = len;
            result.digits = MpdLimbs.digitsOf(result.data, len);
          }
        }
      }
    }

    if (ctx.clamp != 0) {
      int etop = ctx.emax - (ctx.prec - 1);
      if (!result.isSpecial() && result.digits > 0 && result.exp > etop) {
        int pad = result.exp - etop;
        if (result.digits + pad <= ctx.prec) {
          int len = MpdLimbs.mulPow10(
              result.data, result.len, pad, s.a);
          System.arraycopy(s.a, 0, result.data, 0, len);
          result.len = len;
          result.exp = etop;
          result.digits = MpdLimbs.digitsOf(result.data, len);
          ctx.status.set(MpdStatus.CLAMPED);
        }
      }
    }
  }

  private static boolean remainderNonZero(long[] rem, int vLen) {
    int n = Math.max(vLen, 1);
    for (int i = 0; i < n; i++) {
      if (rem[i] != 0L) {
        return true;
      }
    }
    return false;
  }

  private static final long[] ONE = {1L};

  private static boolean shouldRoundUp(
      boolean negative,
      int roundDigit,
      boolean sticky,
      int lsbOdd,
      MpdRound mode,
      int len,
      long[] coeff) {
    if (roundDigit == 0 && !sticky) {
      return false;
    }
    return switch (mode) {
      case DOWN -> false;
      case UP -> true;
      case FLOOR -> negative;
      case CEILING -> !negative;
      case HALF_UP -> roundDigit >= 5;
      case HALF_DOWN -> roundDigit > 5 || (roundDigit == 5 && sticky);
      case HALF_EVEN -> {
        if (roundDigit > 5 || (roundDigit == 5 && sticky)) {
          yield true;
        }
        if (roundDigit < 5) {
          yield false;
        }
        // exactly halfway: round to even LSB of kept coeff
        yield (coeff[0] & 1L) != 0L;
      }
    };
  }

  private static int leadingDiscardedDigit(long[] data, int len, int drop) {
    // digit at position drop-1 from the right (0-based from LSD)
    int pos = drop - 1;
    int limb = pos / Mpd.RDIGITS;
    int dig = pos % Mpd.RDIGITS;
    if (limb >= len) {
      return 0;
    }
    return (int) ((data[limb] / MpdLimbs.POW10[dig]) % 10L);
  }

  private static int discardedSticky(
      long[] data, int len, int drop, int roundDigit) {
    // any digit below the round digit
    if (drop <= 1) {
      return 0;
    }
    int limb = 0;
    long remDigits = drop - 1;
    // check all limbs fully below round digit limb
    int roundLimb = (drop - 1) / Mpd.RDIGITS;
    int roundDig = (drop - 1) % Mpd.RDIGITS;
    for (int i = 0; i < roundLimb && i < len; i++) {
      if (data[i] != 0L) {
        return 1;
      }
    }
    if (roundLimb < len && roundDig > 0) {
      long mask = MpdLimbs.POW10[roundDig];
      if ((data[roundLimb] % mask) != 0L) {
        return 1;
      }
    }
    return 0;
  }


  private static boolean roundOverflowToInf(boolean neg, MpdRound mode) {
    return switch (mode) {
      case DOWN -> false;
      case UP -> true;
      case FLOOR -> neg;
      case CEILING -> !neg;
      default -> true; // half-* -> inf for overflow magnitude
    };
  }

  private static void setMaxFinite(Mpd result, MpdContext ctx) {
    // coeff = 10^prec - 1, exp = emax - prec + 1
    Scratch s = scratch();
    s.a[0] = 0L;
    int len = 1;
    // build 10^prec via mulPow10 from 1
    s.b[0] = 1L;
    len = MpdLimbs.mulPow10(s.b, 1, ctx.prec, s.a);
    // subtract 1
    len = MpdLimbs.subAbs(s.a, len, ONE, 1, s.c);
    System.arraycopy(s.c, 0, result.data, 0, len);
    result.len = len;
    result.digits = ctx.prec;
    result.exp = ctx.emax - ctx.prec + 1;
    // keep sign
    result.flags &= Mpd.SIGN_MASK;
  }
}
