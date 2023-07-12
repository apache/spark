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

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedArgumentExpression}
import org.apache.spark.sql.errors.QueryCompilationErrors

object SupportsNamedArguments {
  /**
   * This method is the default routine which rearranges the arguments in positional order according
   * to the function signature provided. This will also fill in any default values that exists for
   * optional arguments. This method will also be invoked even if there are no named arguments in
   * the argument list.
   *
   * @param functionSignature The function signature that defines the positional ordering
   * @param args The argument list provided in function invocation
   * @param functionName The name of the function
   * @return A list of arguments rearranged in positional order defined by the provided signature
   */
  final def defaultRearrange(
      functionSignature: FunctionSignature,
      args: Seq[Expression],
      functionName: String): Seq[Expression] = {
    val parameters: Seq[NamedArgument] = functionSignature.parameters
    val firstOptionalParamIndex: Int = parameters.indexWhere(_.default.isDefined)
    if (firstOptionalParamIndex != -1 &&
        parameters.drop(firstOptionalParamIndex).exists(_.default.isEmpty)) {
      throw QueryCompilationErrors.unexpectedRequiredParameterInFunctionSignature(
        functionName, functionSignature)
    }

    val firstNamedArgIdx: Int = args.indexWhere(_.isInstanceOf[NamedArgumentExpression])
    val (positionalArgs, namedArgs) =
      if (firstNamedArgIdx == -1) {
        (args, Nil)
      } else {
        args.splitAt(firstNamedArgIdx)
      }
    val namedParameters: Seq[NamedArgument] = parameters.drop(positionalArgs.size)

    // The following loop checks for the following:
    // 1. Unrecognized parameter names
    // 2. Duplicate routine parameter assignments
    val allParameterNames: Seq[String] = parameters.map(_.name)
    val parameterNamesSet: Set[String] = allParameterNames.toSet
    val positionalParametersSet = allParameterNames.take(positionalArgs.size).toSet
    val namedParametersSet = collection.mutable.Set[String]()

    for (arg <- namedArgs) {
      arg match {
        case namedArg: NamedArgumentExpression =>
          val parameterName = namedArg.key
          if (!parameterNamesSet.contains(parameterName)) {
            throw QueryCompilationErrors.unrecognizedParameterName(functionName, namedArg.key,
              parameterNamesSet.toSeq)
          }
          if (positionalParametersSet.contains(parameterName)) {
            throw QueryCompilationErrors.positionalAndNamedArgumentDoubleReference(
              functionName, namedArg.key)
          }
          if (namedParametersSet.contains(parameterName)) {
            throw QueryCompilationErrors.doubleNamedArgumentReference(
              functionName, namedArg.key)
          }
          namedParametersSet.add(namedArg.key)
        case _ =>
          throw QueryCompilationErrors.unexpectedPositionalArgument(functionName)
      }
    }

    // This constructs a map from argument name to value for argument rearrangement.
    val namedArgMap = namedArgs.map { arg =>
      val namedArg = arg.asInstanceOf[NamedArgumentExpression]
      namedArg.key -> namedArg.value
    }.toMap

    // We rearrange named arguments to match their positional order.
    val rearrangedNamedArgs: Seq[Expression] = namedParameters.map { param =>
      namedArgMap.getOrElse(
        param.name,
        if (param.default.isEmpty) {
          throw QueryCompilationErrors.requiredParameterNotFound(functionName, param.name)
        } else {
          param.default.get
        }
      )
    }
    positionalArgs ++ rearrangedNamedArgs
  }
}

/**
 * Represents a parameter of a function expression. Function expressions should use this class
 * to construct the argument lists returned in [[Builder]]
 *
 * @param name     The name of the string.
 * @param default  The default value of the argument. If the default is none, then that means the
 *                 argument is required. If no argument is provided, an exception is thrown.
 */
case class NamedArgument(
    name: String,
    default: Option[Expression] = None)

/**
 * Represents a method signature and the list of arguments it receives as input.
 * Currently, overloads are not supported and only one FunctionSignature is allowed
 * per function expression.
 *
 * @param parameters The list of arguments which the function takes
 */
case class FunctionSignature(parameters: Seq[NamedArgument])
