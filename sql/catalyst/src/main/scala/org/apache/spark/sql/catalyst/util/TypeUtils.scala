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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.types._

/**
 * Helper function to check for valid data types
 */
object TypeUtils {
  def checkForNumericExpr(t: DataType, caller: String): TypeCheckResult = {
    if (t.isInstanceOf[NumericType] || t == NullType) {
      TypeCheckResult.success
    } else {
      TypeCheckResult.fail(s"$caller need numeric type(int, long, double, etc.), not $t")
    }
  }

  def checkForBitwiseExpr(t: DataType, caller: String): TypeCheckResult = {
    if (t.isInstanceOf[IntegralType] || t == NullType) {
      TypeCheckResult.success
    } else {
      TypeCheckResult.fail(s"$caller need integral type(short, int, long, etc.), not $t")
    }
  }

  def checkForOrderingExpr(t: DataType, caller: String): TypeCheckResult = {
    if (t.isInstanceOf[AtomicType] || t == NullType) {
      TypeCheckResult.success
    } else {
      TypeCheckResult.fail(s"$caller need atomic type(binary, boolean, numeric, etc), not $t")
    }
  }

  def getNumeric(t: DataType): Numeric[Any] =
    t.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]

  def getOrdering(t: DataType): Ordering[Any] =
    t.asInstanceOf[AtomicType].ordering.asInstanceOf[Ordering[Any]]
}
