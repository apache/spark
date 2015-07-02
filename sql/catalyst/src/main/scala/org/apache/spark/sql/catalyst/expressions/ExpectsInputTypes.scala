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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.types.DataType


/**
 * An trait that gets mixin to define the expected input types of an expression.
 */
trait ExpectsInputTypes { self: Expression =>

  /**
   * Expected input types from child expressions. The i-th position in the returned seq indicates
   * the type requirement for the i-th child.
   *
   * The possible values at each position are:
   * 1. a specific data type, e.g. LongType, StringType.
   * 2. a non-leaf data type, e.g. NumericType, IntegralType, FractionalType.
   * 3. a list of specific data types, e.g. Seq(StringType, BinaryType).
   */
  def inputTypes: Seq[Any]

  override def checkInputDataTypes(): TypeCheckResult = {
    // We will do the type checking in `HiveTypeCoercion`, so always returning success here.
    TypeCheckResult.TypeCheckSuccess
  }
}

/**
 * Expressions that require a specific `DataType` as input should implement this trait
 * so that the proper type conversions can be performed in the analyzer.
 */
trait AutoCastInputTypes { self: Expression =>

  def inputTypes: Seq[DataType]

  override def checkInputDataTypes(): TypeCheckResult = {
    // We will always do type casting for `AutoCastInputTypes` in `HiveTypeCoercion`,
    // so type mismatch error won't be reported here, but for underling `Cast`s.
    TypeCheckResult.TypeCheckSuccess
  }
}
