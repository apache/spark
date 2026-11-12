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

/** Allocating wrappers used by tests and the harness. */
public final class MpdMath {
  private MpdMath() {
  }

  private static Mpd run1(Tri1 op, Mpd a, MpdContext ctx) {
    Mpd r = new Mpd();
    op.apply(r, a, ctx);
    return r;
  }

  private static Mpd run2(Tri2 op, Mpd a, Mpd b, MpdContext ctx) {
    Mpd r = new Mpd();
    op.apply(r, a, b, ctx);
    return r;
  }

  private interface Tri1 {
    void apply(Mpd r, Mpd a, MpdContext ctx);
  }

  private interface Tri2 {
    void apply(Mpd r, Mpd a, Mpd b, MpdContext ctx);
  }

  // core
  public static Mpd add(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdOps::add, a, b, ctx);
  }
  public static Mpd sub(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdOps::sub, a, b, ctx);
  }
  public static Mpd mul(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdOps::mul, a, b, ctx);
  }
  public static Mpd div(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdOps::div, a, b, ctx);
  }
  public static Mpd sqrt(Mpd a, MpdContext ctx) {
    return run1(MpdOps::sqrt, a, ctx);
  }
  public static Mpd fma(Mpd a, Mpd b, Mpd c, MpdContext ctx) {
    Mpd r = new Mpd();
    MpdOps.fma(r, a, b, c, ctx);
    return r;
  }

  // unary / sign
  public static Mpd abs(Mpd a, MpdContext ctx) {
    return run1(MpdFns::abs, a, ctx);
  }
  public static Mpd plus(Mpd a, MpdContext ctx) {
    return run1(MpdFns::plus, a, ctx);
  }
  public static Mpd minus(Mpd a, MpdContext ctx) {
    return run1(MpdFns::minus, a, ctx);
  }

  // min / max
  public static Mpd max(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdFns::max, a, b, ctx);
  }
  public static Mpd min(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdFns::min, a, b, ctx);
  }
  public static Mpd maxMag(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdFns::maxMag, a, b, ctx);
  }
  public static Mpd minMag(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdFns::minMag, a, b, ctx);
  }

  // integer rounding
  public static Mpd trunc(Mpd a, MpdContext ctx) {
    return run1(MpdFns::trunc, a, ctx);
  }
  public static Mpd floor(Mpd a, MpdContext ctx) {
    return run1(MpdFns::floor, a, ctx);
  }
  public static Mpd ceil(Mpd a, MpdContext ctx) {
    return run1(MpdFns::ceil, a, ctx);
  }
  public static Mpd roundToInt(Mpd a, MpdContext ctx) {
    return run1(MpdFns::roundToInt, a, ctx);
  }

  // scale
  public static Mpd quantize(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdFns::quantize, a, b, ctx);
  }
  public static boolean sameQuantum(Mpd a, Mpd b) {
    return MpdFns.sameQuantum(a, b);
  }
  public static Mpd reduce(Mpd a, MpdContext ctx) {
    return run1(MpdFns::reduce, a, ctx);
  }

  // remainder
  public static Mpd divint(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdFns::divint, a, b, ctx);
  }
  public static Mpd rem(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdFns::rem, a, b, ctx);
  }
  public static Mpd remNear(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdFns::remNear, a, b, ctx);
  }
  public static Mpd[] divmod(Mpd a, Mpd b, MpdContext ctx) {
    Mpd q = new Mpd();
    Mpd r = new Mpd();
    MpdFns.divmod(q, r, a, b, ctx);
    return new Mpd[] {q, r};
  }

  // exponent
  public static Mpd scaleb(Mpd a, Mpd n, MpdContext ctx) {
    return run2(MpdFns::scaleb, a, n, ctx);
  }
  public static Mpd logb(Mpd a, MpdContext ctx) {
    return run1(MpdFns::logb, a, ctx);
  }

  // transcendental
  public static Mpd exp(Mpd a, MpdContext ctx) {
    return run1(MpdTrans::exp, a, ctx);
  }
  public static Mpd ln(Mpd a, MpdContext ctx) {
    return run1(MpdTrans::ln, a, ctx);
  }
  public static Mpd log10(Mpd a, MpdContext ctx) {
    return run1(MpdTrans::log10, a, ctx);
  }
  public static Mpd pow(Mpd a, Mpd b, MpdContext ctx) {
    return run2(MpdTrans::pow, a, b, ctx);
  }
  public static Mpd powmod(Mpd a, Mpd b, Mpd c, MpdContext ctx) {
    Mpd r = new Mpd();
    MpdTrans.powmod(r, a, b, c, ctx);
    return r;
  }
}
