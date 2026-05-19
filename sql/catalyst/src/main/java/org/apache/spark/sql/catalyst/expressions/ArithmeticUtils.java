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

package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.errors.QueryExecutionErrors;

/**
 * Static helpers used by {@code BinaryArithmetic.doGenCode} for ANSI
 * overflow-checked {@code byte} and {@code short} arithmetic. These mirror
 * {@code ByteExactNumeric} / {@code ShortExactNumeric} in {@code numerics.scala}
 * (which the eval path uses); the Java helpers exist only because Scala
 * {@code object} methods can't be called from generated code without going
 * through the {@code references[]} array. Primitive {@code int} / {@code long} /
 * {@code float} / {@code double} arithmetic stays inline -- routing those
 * single-bytecode operations through a static method would be a runtime
 * regression.
 */
public final class ArithmeticUtils {

  private ArithmeticUtils() {}

  // ----- Byte: int arithmetic with overflow check (ANSI) -----

  public static byte byteAddExact(byte a, byte b) {
    int r = a + b;
    if (r < Byte.MIN_VALUE || r > Byte.MAX_VALUE) {
      throw QueryExecutionErrors.binaryArithmeticCauseOverflowError(a, "+", b, "try_add");
    }
    return (byte) r;
  }

  public static byte byteSubtractExact(byte a, byte b) {
    int r = a - b;
    if (r < Byte.MIN_VALUE || r > Byte.MAX_VALUE) {
      throw QueryExecutionErrors.binaryArithmeticCauseOverflowError(a, "-", b, "try_subtract");
    }
    return (byte) r;
  }

  public static byte byteMultiplyExact(byte a, byte b) {
    int r = a * b;
    if (r < Byte.MIN_VALUE || r > Byte.MAX_VALUE) {
      throw QueryExecutionErrors.binaryArithmeticCauseOverflowError(a, "*", b, "try_multiply");
    }
    return (byte) r;
  }

  // ----- Short: int arithmetic with overflow check (ANSI) -----

  public static short shortAddExact(short a, short b) {
    int r = a + b;
    if (r < Short.MIN_VALUE || r > Short.MAX_VALUE) {
      throw QueryExecutionErrors.binaryArithmeticCauseOverflowError(a, "+", b, "try_add");
    }
    return (short) r;
  }

  public static short shortSubtractExact(short a, short b) {
    int r = a - b;
    if (r < Short.MIN_VALUE || r > Short.MAX_VALUE) {
      throw QueryExecutionErrors.binaryArithmeticCauseOverflowError(a, "-", b, "try_subtract");
    }
    return (short) r;
  }

  public static short shortMultiplyExact(short a, short b) {
    int r = a * b;
    if (r < Short.MIN_VALUE || r > Short.MAX_VALUE) {
      throw QueryExecutionErrors.binaryArithmeticCauseOverflowError(a, "*", b, "try_multiply");
    }
    return (short) r;
  }
}
