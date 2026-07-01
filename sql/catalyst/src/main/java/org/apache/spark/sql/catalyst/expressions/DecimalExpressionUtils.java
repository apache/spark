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

import org.apache.spark.QueryContext;
import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.sql.types.Decimal;

/**
 * Static helpers shared by decimal expression {@code doGenCode} (and
 * corresponding eval paths). The codegen invokes these via a single static
 * call, replacing the multi-line inline overflow-handling body.
 */
public final class DecimalExpressionUtils {

  private DecimalExpressionUtils() {}

  /**
   * Apply the target {@code precision}/{@code scale} to a {@code Sum} aggregate
   * result and convert a {@code null} input into the {@code Sum}-specific
   * overflow error. {@code Sum} uses {@code null} in its aggregation buffer to
   * indicate "running total overflowed"; this method either rethrows that as
   * {@code overflowInSumOfDecimalError} or propagates the {@code null}, gated
   * by {@code nullOnOverflow}.
   */
  public static Decimal checkOverflowInSum(
      Decimal value,
      int precision,
      int scale,
      boolean nullOnOverflow,
      QueryContext context) {
    if (value == null) {
      if (nullOnOverflow) return null;
      throw QueryExecutionErrors.overflowInSumOfDecimalError(context, "try_sum");
    }
    return value.toPrecision(precision, scale, Decimal.ROUND_HALF_UP(), nullOnOverflow, context);
  }
}
