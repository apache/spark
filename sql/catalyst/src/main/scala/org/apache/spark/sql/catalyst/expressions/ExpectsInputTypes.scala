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
import org.apache.spark.sql.types.AbstractDataType

/**
 * A trait that gets mixin to define the expected input types of an expression.
 *
 * This trait is typically used by operator expressions (e.g. [[Add]], [[Subtract]]) to define
 * expected input types without any implicit casting.
 *
 * Most function expressions (e.g. [[Substring]] should extends [[ImplicitCastInputTypes]]) instead.
 */
trait ExpectsInputTypes extends Expression {

  /**
   * Expected input types from child expressions. The i-th position in the returned seq indicates
   * the type requirement for the i-th child.
   *
   * The possible values at each position are:
   * 1. a specific data type, e.g. LongType, StringType.
   * 2. a non-leaf abstract data type, e.g. NumericType, IntegralType, FractionalType.
   */
  def inputTypes: Seq[AbstractDataType]

  override def checkInputDataTypes(): TypeCheckResult = {
    val mismatches = children.zip(inputTypes).zipWithIndex.collect {
      case ((child, expected), idx) if !expected.acceptsType(child.dataType) =>
        s"argument ${idx + 1} requires ${expected.simpleString} type, " +
          s"however, '${child.sql}' is of ${child.dataType.simpleString} type."
    }

    if (mismatches.isEmpty) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(mismatches.mkString(" "))
    }
  }
}


/**
 * A mixin for the analyzer to perform implicit type casting using
 * [[org.apache.spark.sql.catalyst.analysis.TypeCoercion.ImplicitTypeCasts]].
 */
trait ImplicitCastInputTypes extends ExpectsInputTypes {
  // No other methods
}
