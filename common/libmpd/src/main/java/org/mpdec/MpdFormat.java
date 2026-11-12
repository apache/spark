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

import java.math.BigDecimal;
import java.math.BigInteger;

/** Parsing and formatting helpers for {@link Mpd}. */
public final class MpdFormat {
  private MpdFormat() {
  }

  public static void parse(Mpd out, String text) {
    if (text == null) {
      throw new IllegalArgumentException("null");
    }
    String s = text.trim();
    if (s.isEmpty()) {
      throw new NumberFormatException(text);
    }
    boolean neg = false;
    int i = 0;
    char c0 = s.charAt(0);
    if (c0 == '+' || c0 == '-') {
      neg = c0 == '-';
      i++;
    }
    String u = s.substring(i).toUpperCase();
    if (u.equals("INF") || u.equals("INFINITY")) {
      out.setInfinite(neg);
      return;
    }
    if (u.equals("NAN") || u.equals("QNAN")) {
      out.setNan(false);
      return;
    }
    if (u.equals("SNAN")) {
      out.setNan(true);
      return;
    }

    int exp = 0;
    int ePos = -1;
    for (int k = i; k < s.length(); k++) {
      char ch = s.charAt(k);
      if (ch == 'e' || ch == 'E') {
        ePos = k;
        break;
      }
    }
    String mant = ePos < 0 ? s.substring(i) : s.substring(i, ePos);
    if (ePos >= 0) {
      exp = Integer.parseInt(s.substring(ePos + 1));
    }
    int dot = mant.indexOf('.');
    String digits;
    int frac = 0;
    if (dot >= 0) {
      frac = mant.length() - dot - 1;
      digits = mant.substring(0, dot) + mant.substring(dot + 1);
    } else {
      digits = mant;
    }
    if (digits.isEmpty() || !digits.chars().allMatch(Character::isDigit)) {
      throw new NumberFormatException(text);
    }
    // strip leading zeros but keep last zero
    int start = 0;
    while (start < digits.length() - 1 && digits.charAt(start) == '0') {
      start++;
    }
    digits = digits.substring(start);
    exp -= frac;
    if (digits.equals("0")) {
      out.setZero(neg);
      out.exp = exp;
      return;
    }
    if (digits.length() > Mpd.MAX_DIGITS) {
      throw new NumberFormatException("too many digits: " + text);
    }
    fromDigitString(out, digits, exp, neg);
  }

  public static Mpd parse(String text) {
    Mpd out = new Mpd();
    parse(out, text);
    return out;
  }

  static void fromDigitString(Mpd out, String digits, int exp, boolean neg) {
    out.flags = neg ? Mpd.SIGN_MASK : 0;
    out.exp = exp;
    out.digits = digits.length();
    long[] data = out.data;
    MpdLimbs.clear(data, 0, out.data.length);
    int len = 0;
    int idx = digits.length();
    while (idx > 0) {
      int start = Math.max(0, idx - Mpd.RDIGITS);
      data[len++] = Long.parseLong(digits.substring(start, idx));
      idx = start;
    }
    out.len = MpdLimbs.normalizeLen(data, len);
  }

  public static String format(Mpd v) {
    if (v.isSnan()) {
      return "sNaN";
    }
    if (v.isNan()) {
      return "NaN";
    }
    if (v.isInfinite()) {
      return v.isNegative() ? "-Infinity" : "Infinity";
    }
    StringBuilder sb = new StringBuilder();
    if (v.isNegative()) {
      sb.append('-');
    }
    if (v.isZero()) {
      sb.append('0');
      if (v.exp != 0) {
        sb.append('E').append(v.exp);
      }
      return sb.toString();
    }
    // scientific: d.dddEadj
    char[] digs = coeffDigits(v);
    sb.append(digs[0]);
    if (digs.length > 1) {
      sb.append('.');
      sb.append(digs, 1, digs.length - 1);
    }
    int adj = v.exp + v.digits - 1;
    sb.append('E').append(adj);
    return sb.toString();
  }

  private static char[] coeffDigits(Mpd v) {
    char[] out = new char[v.digits];
    int pos = v.digits;
    for (int i = 0; i < v.len; i++) {
      long w = v.data[i];
      int chunk = (i == v.len - 1)
          ? ((v.digits - 1) % Mpd.RDIGITS) + 1
          : Mpd.RDIGITS;
      for (int d = 0; d < chunk; d++) {
        out[--pos] = (char) ('0' + (w % 10L));
        w /= 10L;
      }
    }
    return out;
  }

  public static BigDecimal toBigDecimal(Mpd v) {
    if (v.isSpecial()) {
      throw new ArithmeticException("special");
    }
    if (v.isZero()) {
      return BigDecimal.ZERO;
    }
    StringBuilder sb = new StringBuilder(v.digits);
    char[] digs = coeffDigits(v);
    sb.append(digs);
    BigInteger unscaled = new BigInteger(sb.toString());
    if (v.isNegative()) {
      unscaled = unscaled.negate();
    }
    return new BigDecimal(unscaled, -v.exp);
  }

  public static void fromBigDecimal(Mpd out, BigDecimal bd) {
    bd = bd.stripTrailingZeros();
    String plain = bd.unscaledValue().abs().toString();
    boolean neg = bd.signum() < 0;
    int exp = -bd.scale();
    if (plain.equals("0")) {
      out.setZero(neg);
      out.exp = exp;
      return;
    }
    if (plain.length() > Mpd.MAX_DIGITS) {
      // keep caller responsible for rounding via context finalize
      plain = plain.substring(0, Mpd.MAX_DIGITS);
      // This path is used after BigDecimal already rounded.
    }
    fromDigitString(out, plain, exp, neg);
  }
}
