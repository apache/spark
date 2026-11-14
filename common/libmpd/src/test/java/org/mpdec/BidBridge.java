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

import java.lang.reflect.Method;

/**
 * Optional reflective access to a prebuilt libbid jar. Absent classes yield
 * {@link #available()} == false so the harness can skip BID columns.
 */
final class BidBridge {
  private final Class<?> bidCls;
  private final Class<?> roundCls;
  private final Class<?> flagsCls;
  private final Object round;
  private final Method parse;
  private final Method add;
  private final Method sub;
  private final Method mul;
  private final Method div;
  private final Method sqrt;
  private final boolean ok;

  private BidBridge(Class<?> bidCls, Class<?> roundCls, Class<?> flagsCls,
      Object round, Method parse, Method add, Method sub, Method mul,
      Method div, Method sqrt, boolean ok) {
    this.bidCls = bidCls;
    this.roundCls = roundCls;
    this.flagsCls = flagsCls;
    this.round = round;
    this.parse = parse;
    this.add = add;
    this.sub = sub;
    this.mul = mul;
    this.div = div;
    this.sqrt = sqrt;
    this.ok = ok;
  }

  static BidBridge tryLoad(int prec) {
    String clsName = switch (prec) {
      case 16 -> "org.bidfp.Bid64";
      case 34 -> "org.bidfp.Bid128";
      default -> null;
    };
    if (clsName == null) {
      return unavailable();
    }
    try {
      Class<?> bidCls = Class.forName(clsName);
      Class<?> roundCls = Class.forName("org.bidfp.RoundingMode");
      Class<?> flagsCls = Class.forName("org.bidfp.StatusFlags");
      Object round = Enum.valueOf(
          roundCls.asSubclass(Enum.class), "TIES_TO_EVEN");
      Method parse = bidCls.getMethod(
          "parse", String.class, roundCls, flagsCls);
      Method add = bidCls.getMethod("add", bidCls, roundCls, flagsCls);
      Method sub = bidCls.getMethod("subtract", bidCls, roundCls, flagsCls);
      Method mul = bidCls.getMethod("multiply", bidCls, roundCls, flagsCls);
      Method div = bidCls.getMethod("divide", bidCls, roundCls, flagsCls);
      Method sqrt = bidCls.getMethod("sqrt", roundCls, flagsCls);
      return new BidBridge(
          bidCls, roundCls, flagsCls, round, parse, add, sub, mul, div, sqrt,
          true);
    } catch (ReflectiveOperationException e) {
      return unavailable();
    }
  }

  private static BidBridge unavailable() {
    return new BidBridge(
        null, null, null, null, null, null, null, null, null, null, false);
  }

  boolean available() {
    return ok;
  }

  Object[] parseAll(String[] texts) throws ReflectiveOperationException {
    Object[] out = new Object[texts.length];
    for (int i = 0; i < texts.length; i++) {
      Object flags = flagsCls.getConstructor().newInstance();
      out[i] = parse.invoke(null, texts[i], round, flags);
    }
    return out;
  }

  Object add(Object a, Object b, Object flags)
      throws ReflectiveOperationException {
    return add.invoke(a, b, round, flags);
  }

  Object sub(Object a, Object b, Object flags)
      throws ReflectiveOperationException {
    return sub.invoke(a, b, round, flags);
  }

  Object mul(Object a, Object b, Object flags)
      throws ReflectiveOperationException {
    return mul.invoke(a, b, round, flags);
  }

  Object div(Object a, Object b, Object flags)
      throws ReflectiveOperationException {
    return div.invoke(a, b, round, flags);
  }

  Object sqrt(Object a, Object flags) throws ReflectiveOperationException {
    return sqrt.invoke(a, round, flags);
  }

  Object newFlags() throws ReflectiveOperationException {
    return flagsCls.getConstructor().newInstance();
  }
}
