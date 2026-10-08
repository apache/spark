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
package org.bidfp.binary128;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Compares {@link Dpml} against Intel DPML {@code bid_f128_*} packed results.
 * Arithmetic allows 1 ULP vs Intel (NaN payloads may differ). Libm kernels
 * here are series approximations without Intel tables, so only specials are
 * required to match; other libm lines are evaluated to catch crashes.
 */
final class IntelF128OracleTest {
  @Test
  void arithmeticMatchesIntelDpml() throws IOException {
    int checked = 0;
    for (IntelF128Oracle.Case c : IntelF128Oracle.load()) {
      if (!isArithmetic(c.op)) {
        continue;
      }
      Binary128 got = eval(c);
      int ulp = IntelF128Oracle.ulpDistance(c.expected, got);
      assertTrue(ulp <= 1, mismatch(c, got) + " ulp=" + ulp);
      checked++;
    }
    assertTrue(checked > 0, "no arithmetic vectors");
  }

  @Test
  void libmSpecialsMatchAndOthersEvaluate() throws IOException {
    List<IntelF128Oracle.Case> cases = IntelF128Oracle.load();
    int specials = 0;
    for (IntelF128Oracle.Case c : cases) {
      if (isArithmetic(c.op)) {
        continue;
      }
      Binary128 got = eval(c);
      if (isRequiredSpecial(c)) {
        assertTrue(
            IntelF128Oracle.sameBitsOrBothNaN(c.expected, got),
            mismatch(c, got));
        specials++;
      }
    }
    assertTrue(specials > 0, "no libm specials");
  }

  private static boolean isArithmetic(String op) {
    return "add".equals(op) || "sub".equals(op) || "mul".equals(op)
        || "div".equals(op) || "sqrt".equals(op);
  }

  private static boolean isRequiredSpecial(IntelF128Oracle.Case c) {
    if (c.binary()) {
      return false;
    }
    boolean z = c.x.isZero() && !c.x.isSigned();
    boolean one = c.x.equals(Binary128.ONE);
    if (z && ("exp".equals(c.op) || "exp2".equals(c.op) || "exp10".equals(c.op)
        || "expm1".equals(c.op) || "sin".equals(c.op) || "tan".equals(c.op)
        || "atan".equals(c.op) || "cbrt".equals(c.op) || "erf".equals(c.op)
        || "sinh".equals(c.op) || "tanh".equals(c.op) || "asinh".equals(c.op)
        || "atanh".equals(c.op))) {
      return true;
    }
    if (z && "cos".equals(c.op)) {
      return true;
    }
    return one && ("log".equals(c.op) || "log2".equals(c.op)
        || "log10".equals(c.op));
  }

  private static Binary128 eval(IntelF128Oracle.Case c) {
    StatusFlags st = new StatusFlags();
    RoundingMode mode = RoundingMode.fromIntel(c.rnd);
    switch (c.op) {
      case "add":
        return Dpml.add(c.x, c.y, mode, st);
      case "sub":
        return Dpml.sub(c.x, c.y, mode, st);
      case "mul":
        return Dpml.mul(c.x, c.y, mode, st);
      case "div":
        return Dpml.div(c.x, c.y, mode, st);
      case "sqrt":
        return Dpml.sqrt(c.x, mode, st);
      case "exp":
        return Dpml.exp(c.x, mode, st);
      case "expm1":
        return Dpml.expm1(c.x, mode, st);
      case "exp2":
        return Dpml.exp2(c.x, mode, st);
      case "exp10":
        return Dpml.exp10(c.x, mode, st);
      case "log":
        return Dpml.log(c.x, mode, st);
      case "log2":
        return Dpml.log2(c.x, mode, st);
      case "log10":
        return Dpml.log10(c.x, mode, st);
      case "log1p":
        return Dpml.log1p(c.x, mode, st);
      case "pow":
        return Dpml.pow(c.x, c.y, mode, st);
      case "cbrt":
        return Dpml.cbrt(c.x, mode, st);
      case "sin":
        return Dpml.sin(c.x, mode, st);
      case "cos":
        return Dpml.cos(c.x, mode, st);
      case "tan":
        return Dpml.tan(c.x, mode, st);
      case "asin":
        return Dpml.asin(c.x, mode, st);
      case "acos":
        return Dpml.acos(c.x, mode, st);
      case "atan":
        return Dpml.atan(c.x, mode, st);
      case "sinh":
        return Dpml.sinh(c.x, mode, st);
      case "cosh":
        return Dpml.cosh(c.x, mode, st);
      case "tanh":
        return Dpml.tanh(c.x, mode, st);
      case "asinh":
        return Dpml.asinh(c.x, mode, st);
      case "acosh":
        return Dpml.acosh(c.x, mode, st);
      case "atanh":
        return Dpml.atanh(c.x, mode, st);
      case "erf":
        return Dpml.erf(c.x, mode, st);
      case "erfc":
        return Dpml.erfc(c.x, mode, st);
      case "lgamma":
        return Dpml.lgamma(c.x, mode, st);
      case "tgamma":
        return Dpml.tgamma(c.x, mode, st);
      default:
        throw new IllegalArgumentException(c.op);
    }
  }

  private static String mismatch(IntelF128Oracle.Case c, Binary128 got) {
    return c.op + " rnd=" + c.rnd + " x=" + c.x + (c.y != null ? " y=" + c.y : "")
        + " expected=" + c.expected + " got=" + got;
  }
}
