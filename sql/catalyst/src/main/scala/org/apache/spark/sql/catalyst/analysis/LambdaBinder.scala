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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Expression, LambdaFunction, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.util.TypeUtils.{toSQLConf, toSQLId}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * Object used to bind lambda function arguments to their types and validate lambda argument
 * constraints.
 *
 * This object creates a bound [[LambdaFunction]] by binding the arguments to the given type
 * information (dataType and nullability). The argument names come from the lambda function
 * itself. It handles three cases:
 *
 * 1. Already bound lambda functions: Returns the function as-is, assuming it has been
 *    correctly bound to its arguments.
 *
 * 2. Unbound lambda functions: Validates and binds the function by:
 *    - Checking that the number of arguments matches the expected count
 *    - Checking for duplicate argument names (respecting case sensitivity configuration)
 *    - Creating [[NamedLambdaVariable]] instances with the provided types
 *
 * 3. Non-lambda expressions: Wraps the expression in a lambda function with hidden arguments
 *    (named `col0`, `col1`, etc.). This is used when an expression does not consume lambda
 *    arguments but needs to be passed to a higher-order function. The arguments are hidden to
 *    prevent accidental naming collisions.
 */
object LambdaBinder extends SQLConfHelper {

  /**
   * Binds lambda function arguments to their types and validates lambda argument constraints.
   */
  def apply(expression: Expression, argumentsInfo: Seq[(DataType, Boolean)]): LambdaFunction =
    expression match {
      case f: LambdaFunction if f.bound => f

      case LambdaFunction(function, names, _) =>
        if (names.size != argumentsInfo.size) {
          expression.failAnalysis(
            errorClass = "INVALID_LAMBDA_FUNCTION_CALL.NUM_ARGS_MISMATCH",
            messageParameters = Map(
              "expectedNumArgs" -> names.size.toString,
              "actualNumArgs" -> argumentsInfo.size.toString
            )
          )
        }

        if (names.map(a => conf.canonicalize(a.name)).distinct.size < names.size) {
          expression.failAnalysis(
            errorClass = "INVALID_LAMBDA_FUNCTION_CALL.DUPLICATE_ARG_NAMES",
            messageParameters = Map(
              "args" -> names.map(a => conf.canonicalize(a.name)).map(toSQLId(_)).mkString(", "),
              "caseSensitiveConfig" -> toSQLConf(SQLConf.CASE_SENSITIVE.key)
            )
          )
        }

        val arguments = argumentsInfo.zip(names).map {
          case ((dataType, nullable), ne) =>
            NamedLambdaVariable(ne.name, dataType, nullable)
        }
        LambdaFunction(function, arguments)

      case _ =>
        val arguments = argumentsInfo.zipWithIndex.map {
          case ((dataType, nullable), i) =>
            NamedLambdaVariable(s"col$i", dataType, nullable)
        }
        LambdaFunction(expression, arguments, hidden = true)
    }
}
