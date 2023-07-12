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
import org.apache.spark.sql.types.AbstractDataType

object SupportsNamedArguments {
  final def defaultRearrange(functionSignature: FunctionSignature,
      args: Seq[Expression],
      functionName: String): Seq[Expression] = {
    val parameters: Seq[NamedArgument] = functionSignature.parameters
    val firstNamedArgIdx: Int = args.indexWhere(_.isInstanceOf[NamedArgumentExpression])
    val (positionalArgs, namedArgs) =
      if (firstNamedArgIdx == -1) {
        (args, Nil)
      } else {
        args.splitAt(firstNamedArgIdx)
      }
    val namedParameters: Seq[NamedArgument] = parameters.drop(positionalArgs.size)

    // Performing some checking to ensure valid argument list
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

    // Construct a map from argument name to value for argument rearrangement
    val namedArgMap = namedArgs.map { arg =>
      val namedArg = arg.asInstanceOf[NamedArgumentExpression]
      namedArg.key -> namedArg.value
    }.toMap

    // Rearrange named arguments to match their positional order
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
 * Identifies which forms of provided argument values are expected for each call
 * to the associated SQL function
 */
trait NamedArgumentType

/**
 * Represents a named argument that expects a scalar value of one specific DataType
 *
 * @param dataType The data type of some argument
 */
case class FixedArgumentType(dataType: AbstractDataType) extends NamedArgumentType

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
