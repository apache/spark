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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  CreateArray,
  CreateMap,
  CreateNamedStruct,
  Expression,
  Literal,
  MapFromArrays,
  MapFromEntries,
  VariableReference
}

/**
 * Object used to validate arguments of [[ParameterizedQuery]] nodes.
 */
object ParameterizedQueryArgumentsValidator {

  /**
   * Validates the list of provided arguments. In case there is a invalid argument, throws
   * `INVALID_SQL_ARG` exception.
   */
  def apply(arguments: Iterable[(String, Expression)]): Unit = {
    arguments.find(arg => isNotAllowed(arg._2)).foreach { case (name, expr) =>
      expr.failAnalysis(
        errorClass = "INVALID_SQL_ARG",
        messageParameters = Map("name" -> name))
    }
  }

  /**
   * Recursively checks the provided expression tree. In case there is an invalid expression type
   * returns `false`. Otherwise, returns `true`.
   */
  private def isNotAllowed(expression: Expression): Boolean = expression.exists {
    case _: Literal | _: CreateArray | _: CreateNamedStruct | _: CreateMap | _: MapFromArrays |
        _: MapFromEntries | _: VariableReference | _: Alias =>
      false
    case _ => true
  }
}
