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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Static helpers used by {@code Cast.doGenCode} (and corresponding eval
 * paths) for ANSI overflow-checked narrowing to {@code byte} / {@code short}.
 *
 * <p>Narrowing to {@code int} / {@code long} is handled by calling the existing
 * {@code LongExactNumeric} / {@code FloatExactNumeric} / {@code DoubleExactNumeric}
 * Scala objects directly from codegen (see SPARK-56909). The helpers below
 * cover {@code byte} / {@code short} only, since {@code ByteExactNumeric} /
 * {@code ShortExactNumeric} don't expose a cross-type narrowing API.
 *
 * <p>The source and target {@link DataType} objects referenced by the overflow
 * error message are held in {@code private static final} fields so the happy
 * path performs no per-row {@code references[]} lookups.
 */
public final class CastUtils {

  private CastUtils() {}

  private static final DataType SHORT = DataTypes.ShortType;
  private static final DataType INT = DataTypes.IntegerType;
  private static final DataType LONG = DataTypes.LongType;
  private static final DataType BYTE = DataTypes.ByteType;
  private static final DataType FLOAT = DataTypes.FloatType;
  private static final DataType DOUBLE = DataTypes.DoubleType;

  // ----- integral narrowing (ANSI: throw on overflow) -----

  public static byte shortToByteExact(short v) {
    if (v == (byte) v) return (byte) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, SHORT, BYTE);
  }

  public static byte intToByteExact(int v) {
    if (v == (byte) v) return (byte) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, INT, BYTE);
  }

  public static byte longToByteExact(long v) {
    if (v == (byte) v) return (byte) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, LONG, BYTE);
  }

  public static short intToShortExact(int v) {
    if (v == (short) v) return (short) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, INT, SHORT);
  }

  public static short longToShortExact(long v) {
    if (v == (short) v) return (short) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, LONG, SHORT);
  }

  // ----- fractional -> integral (ANSI: throw on overflow) -----
  // Mirrors castFractionToIntegralTypeCode: floor(v) <= MAX && ceil(v) >= MIN.

  public static byte floatToByteExact(float v) {
    if (Math.floor(v) <= Byte.MAX_VALUE && Math.ceil(v) >= Byte.MIN_VALUE) return (byte) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, FLOAT, BYTE);
  }

  public static byte doubleToByteExact(double v) {
    if (Math.floor(v) <= Byte.MAX_VALUE && Math.ceil(v) >= Byte.MIN_VALUE) return (byte) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, DOUBLE, BYTE);
  }

  public static short floatToShortExact(float v) {
    if (Math.floor(v) <= Short.MAX_VALUE && Math.ceil(v) >= Short.MIN_VALUE) return (short) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, FLOAT, SHORT);
  }

  public static short doubleToShortExact(double v) {
    if (Math.floor(v) <= Short.MAX_VALUE && Math.ceil(v) >= Short.MIN_VALUE) return (short) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, DOUBLE, SHORT);
  }
}
