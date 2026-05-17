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
 * paths) for ANSI overflow-checked narrowing conversions. The source and
 * target {@link DataType} objects referenced by the overflow error message
 * are held in {@code private static final} fields so the happy path
 * performs no per-row {@code references[]} lookups.
 */
public final class CastUtils {

  private CastUtils() {}

  private static final DataType INT = DataTypes.IntegerType;
  private static final DataType LONG = DataTypes.LongType;
  private static final DataType FLOAT = DataTypes.FloatType;
  private static final DataType DOUBLE = DataTypes.DoubleType;

  // ----- integral narrowing -> int (ANSI: throw on overflow) -----

  public static int longToIntExact(long v) {
    if (v == (int) v) return (int) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, LONG, INT);
  }

  // ----- fractional -> int (ANSI: throw on overflow) -----
  // Mirrors castFractionToIntegralTypeCode: floor(v) <= MAX && ceil(v) >= MIN.

  public static int floatToIntExact(float v) {
    if (Math.floor(v) <= Integer.MAX_VALUE && Math.ceil(v) >= Integer.MIN_VALUE) return (int) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, FLOAT, INT);
  }

  public static int doubleToIntExact(double v) {
    if (Math.floor(v) <= Integer.MAX_VALUE && Math.ceil(v) >= Integer.MIN_VALUE) return (int) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, DOUBLE, INT);
  }

  // ----- fractional -> long (ANSI: throw on overflow) -----

  public static long floatToLongExact(float v) {
    if (Math.floor(v) <= Long.MAX_VALUE && Math.ceil(v) >= Long.MIN_VALUE) return (long) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, FLOAT, LONG);
  }

  public static long doubleToLongExact(double v) {
    if (Math.floor(v) <= Long.MAX_VALUE && Math.ceil(v) >= Long.MIN_VALUE) return (long) v;
    throw QueryExecutionErrors.castingCauseOverflowError(v, DOUBLE, LONG);
  }
}
