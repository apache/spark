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
 * Transcendental quiet operations: exp, ln, log10, pow, powmod.
 * Precision-capped series / Newton implementations for the evaluation port.
 */
public final class MpdTrans {
  private MpdTrans() {
  }

  private static final String LN10 =
      "2.302585092994045684017991454684364207601101488628772976033";

  public static void exp(Mpd result, Mpd a, MpdContext ctx) {
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
      if (a.isNegative()) {
        result.setZero(false);
      } else {
        result.setInfinite(false);
      }
      return;
    }
    if (a.isZero()) {
      fromOne(result);
      return;
    }

    MpdOps.Scratch s = MpdOps.scratch();
    int workPrec = Math.min(Mpd.MAX_DIGITS, ctx.prec + 4);
    MpdContext work = new MpdContext(
        workPrec, ctx.emax, ctx.emin, MpdRound.HALF_EVEN, ctx.clamp);

    Mpd ln10 = MpdFormat.parse(
        LN10.substring(0, Math.min(LN10.length(), workPrec + 2)));
    Mpd y = Mpd.work();
    Mpd nMp = Mpd.work();
    Mpd frac = Mpd.work();
    Mpd r = Mpd.work();
    MpdOps.div(y, a, ln10, work);
    MpdFns.floor(nMp, y, work);
    MpdOps.sub(frac, y, nMp, work);
    long n = MpdFns.intValueExact(nMp);
    if (n == Long.MIN_VALUE) {
      if (!nMp.isNegative()) {
        result.setInfinite(false);
        ctx.status.set(
            MpdStatus.OVERFLOW | MpdStatus.INEXACT | MpdStatus.ROUNDED);
      } else {
        result.setZero(false);
        ctx.status.set(
            MpdStatus.UNDERFLOW | MpdStatus.INEXACT | MpdStatus.ROUNDED);
      }
      return;
    }
    MpdOps.mul(r, frac, ln10, work);
    Mpd series = Mpd.work();
    expSeries(series, r, work);
    series.exp += (int) n;
    result.copyFrom(series);
    MpdOps.finish(result, ctx, s);
  }

  /** Taylor exp(r) = sum r^k / k!. */
  private static void expSeries(Mpd result, Mpd r, MpdContext work) {
    fromOne(result);
    if (r.isZero()) {
      return;
    }
    Mpd term = Mpd.work();
    Mpd tmp = Mpd.work();
    Mpd k = Mpd.work();
    fromOne(term);
    for (int i = 1; i < work.prec * 2 + 10; i++) {
      MpdFns.fromInt(k, i);
      MpdOps.mul(tmp, term, r, work);
      MpdOps.div(term, tmp, k, work);
      MpdOps.add(tmp, result, term, work);
      result.copyFrom(tmp);
      if (term.isZero()
          || term.adjExp() < result.adjExp() - work.prec - 2) {
        break;
      }
    }
  }

  public static void ln(Mpd result, Mpd a, MpdContext ctx) {
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
    if (a.isZero()) {
      result.setInfinite(true);
      ctx.status.set(MpdStatus.DIVISION_BY_ZERO);
      return;
    }
    if (a.isInfinite()) {
      result.setInfinite(false);
      return;
    }
    if (isOne(a)) {
      result.setZero(false);
      return;
    }

    MpdOps.Scratch s = MpdOps.scratch();
    int workPrec = Math.min(Mpd.MAX_DIGITS, ctx.prec + 4);
    MpdContext work = new MpdContext(
        workPrec, ctx.emax, ctx.emin, MpdRound.HALF_EVEN, ctx.clamp);

    int e = a.adjExp();
    Mpd m = Mpd.work();
    m.copyFrom(a);
    m.exp = a.exp - e;
    // m == 1 (possibly unnormalized, e.g. 10 * 10^-1)
    Mpd unity = Mpd.work();
    fromOne(unity);
    if (MpdOps.compare(m, unity) == 0) {
      if (e == 0) {
        result.setZero(false);
      } else {
        Mpd ln10 = MpdFormat.parse(
            LN10.substring(0, Math.min(LN10.length(),
                Math.min(Mpd.MAX_DIGITS, ctx.prec + 4) + 2)));
        Mpd eMp = Mpd.work();
        MpdFns.fromInt(eMp, e);
        MpdOps.mul(result, eMp, ln10, ctx);
      }
      return;
    }

    double approx = Math.log(toDoubleApprox(m));
    if (!Double.isFinite(approx) || approx == 0.0) {
      approx = m.data[0] > 1L ? 1.0 : -0.5;
    }
    MpdFormat.parse(result, Double.toString(approx));

    Mpd ey = Mpd.work();
    Mpd negY = Mpd.work();
    Mpd tmp = Mpd.work();
    Mpd one = Mpd.work();
    fromOne(one);
    int iters = 2 + (workPrec / 16);
    for (int i = 0; i < iters; i++) {
      // y' = y + m*exp(-y) - 1
      negY.copyFrom(result);
      negY.flags ^= Mpd.SIGN_MASK;
      expSeries(ey, negY, work);
      MpdOps.mul(tmp, m, ey, work);
      MpdOps.sub(ey, tmp, one, work);
      MpdOps.add(tmp, result, ey, work);
      result.copyFrom(tmp);
    }
    if (e != 0) {
      Mpd ln10 = MpdFormat.parse(
          LN10.substring(0, Math.min(LN10.length(), workPrec + 2)));
      Mpd eMp = Mpd.work();
      MpdFns.fromInt(eMp, e);
      MpdOps.mul(tmp, eMp, ln10, work);
      MpdOps.add(ey, result, tmp, work);
      result.copyFrom(ey);
    }
    MpdOps.finish(result, ctx, s);
  }

  public static void log10(Mpd result, Mpd a, MpdContext ctx) {
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
    if (a.isZero()) {
      result.setInfinite(true);
      ctx.status.set(MpdStatus.DIVISION_BY_ZERO);
      return;
    }
    if (a.isInfinite()) {
      result.setInfinite(false);
      return;
    }
    MpdOps.Scratch s = MpdOps.scratch();
    // a = m * 10^e with m in [1, 10). log10(a) = e + log10(m) = e + ln(m)/ln(10)
    int e = a.adjExp();
    Mpd m = Mpd.work();
    m.copyFrom(a);
    m.exp = a.exp - e;
    // m == 1 exactly?
    if (m.digits == 1 && m.data[0] == 1L && m.exp == 0) {
      MpdFns.fromInt(result, e);
      MpdOps.finish(result, ctx, s);
      return;
    }
    // Also 10...0 * 10^(1-digits) == 1
    {
      Mpd red = Mpd.work();
      MpdFns.reduce(red, m, ctx);
      if (red.digits == 1 && red.data[0] == 1L && red.exp == 0) {
        MpdFns.fromInt(result, e);
        MpdOps.finish(result, ctx, s);
        return;
      }
    }
    int workPrec = Math.min(Mpd.MAX_DIGITS, ctx.prec + 4);
    MpdContext work = new MpdContext(
        workPrec, ctx.emax, ctx.emin, MpdRound.HALF_EVEN, ctx.clamp);
    Mpd lnm = Mpd.work();
    // Force ln of mantissa only (adjExp 0 path): call ln on m with work
    // Rebuild m as value in [1,10) with adjExp 0 by using parse of leading digits.
    ln(lnm, m, work);
    Mpd ln10 = MpdFormat.parse(
        LN10.substring(0, Math.min(LN10.length(), workPrec + 2)));
    Mpd frac = Mpd.work();
    MpdOps.div(frac, lnm, ln10, work);
    Mpd eMp = Mpd.work();
    MpdFns.fromInt(eMp, e);
    MpdOps.add(result, eMp, frac, ctx);
  }

  public static void pow(Mpd result, Mpd base, Mpd exp, MpdContext ctx) {
    if (base.isSnan() || exp.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (base.isNan() || exp.isNan()) {
      result.setNan(false);
      return;
    }
    if (exp.isZero()) {
      fromOne(result);
      return;
    }
    if (isOne(base)) {
      fromOne(result);
      return;
    }
    if (base.isZero()) {
      if (exp.isNegative()) {
        result.setInfinite(false);
        ctx.status.set(MpdStatus.DIVISION_BY_ZERO);
      } else {
        result.setZero(false);
      }
      return;
    }
    // Integral-valued exponents such as 3.0 take the exact integer path.
    long eInt = Long.MIN_VALUE;
    if (MpdFns.isIntegral(exp)) {
      Mpd expInt = Mpd.work();
      MpdContext exact = new MpdContext(
          Mpd.MAX_DIGITS, ctx.emax, ctx.emin, MpdRound.DOWN, 0);
      MpdFns.trunc(expInt, exp, exact);
      eInt = MpdFns.intValueExact(expInt);
    }
    if (eInt != Long.MIN_VALUE && Math.abs(eInt) <= 1000L) {
      powInt(result, base, eInt, ctx);
      return;
    }
    if (base.isNegative()) {
      result.setNan(false);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    int workPrec = Math.min(Mpd.MAX_DIGITS, ctx.prec + 4);
    MpdContext work = new MpdContext(
        workPrec, ctx.emax, ctx.emin, MpdRound.HALF_EVEN, ctx.clamp);
    Mpd lnv = Mpd.work();
    Mpd prod = Mpd.work();
    ln(lnv, base, work);
    MpdOps.mul(prod, exp, lnv, work);
    exp(result, prod, ctx);
  }

  private static void powInt(Mpd result, Mpd base, long e, MpdContext ctx) {
    boolean negExp = e < 0;
    long n = negExp ? -e : e;
    fromOne(result);
    Mpd b = Mpd.work();
    Mpd tmp = Mpd.work();
    b.copyFrom(base);
    while (n > 0L) {
      if ((n & 1L) != 0L) {
        MpdOps.mul(tmp, result, b, ctx);
        result.copyFrom(tmp);
      }
      n >>= 1;
      if (n > 0L) {
        MpdOps.mul(tmp, b, b, ctx);
        b.copyFrom(tmp);
      }
    }
    if (negExp) {
      Mpd one = Mpd.work();
      fromOne(one);
      MpdOps.div(tmp, one, result, ctx);
      result.copyFrom(tmp);
    }
  }

  public static void powmod(
      Mpd result, Mpd base, Mpd exp, Mpd mod, MpdContext ctx) {
    if (base.isSnan() || exp.isSnan() || mod.isSnan()) {
      result.setNan(true);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    if (base.isNan() || exp.isNan() || mod.isNan()) {
      result.setNan(false);
      return;
    }
    // Same contract as libmpdec mpd_qpowmod: all three must be integers
    // (2.0 is an integer, 2.5 is not), mod non-zero and fitting in prec,
    // exponent non-negative.
    if (!MpdFns.isIntegral(base) || !MpdFns.isIntegral(exp)
        || !MpdFns.isIntegral(mod) || mod.isZero()
        || mod.digits + mod.exp > ctx.prec || exp.isNegative()) {
      result.setNan(false);
      ctx.status.set(MpdStatus.INVALID_OPERATION);
      return;
    }
    fromOne(result);
    Mpd b = Mpd.work();
    Mpd tmp = Mpd.work();
    // Normalize exponent to a non-negative integer (exp field 0) so the
    // low-digit parity test below is valid for values like 3.0.
    Mpd e = Mpd.work();
    MpdFns.trunc(e, exp, ctx);
    Mpd two = Mpd.work();
    MpdFns.fromInt(two, 2);
    MpdFns.rem(b, base, mod, ctx);
    while (!e.isZero()) {
      if ((e.data[0] % 2L) != 0L) {
        MpdOps.mul(tmp, result, b, ctx);
        MpdFns.rem(result, tmp, mod, ctx);
      }
      Mpd half = Mpd.work();
      MpdFns.divint(half, e, two, ctx);
      e.copyFrom(half);
      if (e.isZero()) {
        break;
      }
      MpdOps.mul(tmp, b, b, ctx);
      MpdFns.rem(b, tmp, mod, ctx);
    }
  }

  private static void fromOne(Mpd out) {
    out.clearFinite();
    out.data[0] = 1L;
    out.len = 1;
    out.digits = 1;
    out.exp = 0;
  }

  private static boolean isOne(Mpd a) {
    return !a.isSpecial() && !a.isNegative()
        && a.digits == 1 && a.data[0] == 1L && a.exp == 0;
  }

  private static double toDoubleApprox(Mpd a) {
    long[] buf = new long[Mpd.WORK_LIMBS];
    int drop = Math.max(0, a.digits - 15);
    MpdLimbs.Result rr = MpdLimbs.divPow10(a.data, a.len, drop, buf);
    double coeff = 0.0;
    for (int i = rr.len - 1; i >= 0; i--) {
      coeff = coeff * (double) Mpd.RADIX + (double) buf[i];
    }
    return coeff * Math.pow(10.0, a.exp + drop);
  }
}
