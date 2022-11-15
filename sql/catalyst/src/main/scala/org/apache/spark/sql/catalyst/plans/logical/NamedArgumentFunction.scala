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

package org.apache.spark.sql.catalyst.plans.logical

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedArgumentExpression}
import org.apache.spark.sql.errors.QueryCompilationErrors.{tableFunctionDuplicateNamedArguments, tableFunctionUnexpectedArgument}
import org.apache.spark.sql.types._

/**
 * A trait to define a named argument function:
 * Usage: _FUNC_(arg0, arg1, arg2, arg5 => value5, arg8 => value8)
 *
 * - Arguments can be passed positionally or by name
 * - Positional arguments cannot come after a named argument
 */
trait NamedArgumentFunction {
  /**
   * A trait [[Param]] that is used to define function parameter
   * - name: case insensitive name
   * - dataType: expected data type
   * - required: if the parameter is required
   * - default: the default value of the parameter if not provided
   */
  protected trait Param {
    val name: String
    val dataType: DataType
    val required: Boolean
    val default: Option[Any]
  }

  /**
   * Function parameters
   */
  def parameters: Seq[Param]

  /**
   * Input expressions
   */
  def inputExpressions: Seq[Expression]

  /**
   * positionalArguments: positional arguments extracted from input expressions
   * namedArguments: named arguments extracted from input expressions
   */
  lazy val (positionalArguments, namedArguments):
    (Seq[Expression], Seq[NamedArgumentExpression]) = {
    val positionalBuffer = mutable.ArrayBuffer[Expression]()
    val namedBuffer = mutable.ArrayBuffer[NamedArgumentExpression]()
    val namedSet = mutable.Set[String]()
    inputExpressions.zipWithIndex.foreach {
      case (NamedArgumentExpression(key, _), i)
        if namedSet.contains(key.toLowerCase(Locale.ROOT)) =>
        throw tableFunctionDuplicateNamedArguments(key, i)
      case (kv @ NamedArgumentExpression(key, _), _) =>
        namedSet.add(key.toLowerCase(Locale.ROOT))
        namedBuffer += kv
      case (e: Expression, i) if namedBuffer.nonEmpty =>
        throw tableFunctionUnexpectedArgument(e, i)
      case (e: Expression, _) => positionalBuffer += e
    }
    (positionalBuffer, namedBuffer)
  }

  /**
   * Sanity check: cannot define duplicated function parameters
   */
  private def checkDuplicatedParameters(): Unit = {
    assert(parameters.map(_.name.toLowerCase(Locale.ROOT)).toSet.size == parameters.size,
      "Cannot define duplicated parameters in named argument function.")
  }

  checkDuplicatedParameters()
}
