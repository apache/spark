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

/**
 * The class which companion objects of function expression may implement to
 * support named arguments for that function expression. Please note that variadic final
 * arguments are NOT supported for named arguments. Do not use for functions that
 * has variadic final arguments!
 *
 * Example:
 *  object CountMinSketchAgg extends SupportsNamedArguments {
 *    final val functionSignature = FunctionSignature(Seq(
 *      NamedArgument("column",
 *          FixedArgumentType(TypeCollection(IntegralType, StringType, BinaryType))),
 *      NamedArgument("epsilon", FixedArgumentType(DoubleType)),
 *      NamedArgument("confidence", FixedArgumentType(DoubleType)),
 *      NamedArgument("seed", FixedArgumentType(IntegerType))
 *    ))
 *    override def functionSignatures: Seq[FunctionSignature] = Seq(functionSignature)
 *  }
 */
abstract class SupportsNamedArguments {
  /**
   * This is the method overridden by function expressions to define their method signatures.
   * Currently, we don't support overloads, so we restrict each function expression to return
   * only one FunctionSignature.
   *
   * @return the signature of the function expression
   */
  def functionSignatures: Seq[FunctionSignature]

  /**
   * This function rearranges the list of expressions according to the function signature
   * It is recommended to use this provided implementation as it is consistent with
   * the SQL standard. If absolutely necessary the developer can choose to override the default
   * behavior for additional flexibility.
   *
   * @param expectedSignature Function signature that denotes positional order of arguments
   * @param providedArguments The sequence of expressions from function invocation
   * @param functionName The name of the function invoked for debugging purposes
   * @return positional order of arguments according to FunctionSignature obtained
   *         by changing the order of the above provided arguments
   */
  protected def rearrange(
      expectedSignature: FunctionSignature,
      providedArguments: Seq[Expression],
      functionName: String): Seq[Expression] = {
    SupportsNamedArguments.defaultRearrange(expectedSignature, providedArguments, functionName)
  }
}

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
    val assignedParameterSet = collection.mutable.Set[String](
      allParameterNames.take(positionalArgs.size): _*)
    for (arg <- namedArgs) {
      arg match {
        case namedArg: NamedArgumentExpression =>
          if (assignedParameterSet.contains(namedArg.key)) {
            throw QueryCompilationErrors.duplicateRoutineParameterAssignment(
              functionName, namedArg.key)
          }
          if (!parameterNamesSet.contains(namedArg.key)) {
            throw QueryCompilationErrors.unrecognizedParameterName(functionName, namedArg.key,
              parameterNamesSet.toSeq)
          }
          assignedParameterSet.add(namedArg.key)
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
 * to construct the argument lists returned in [[SupportsNamedArguments.functionSignatures]]
 *
 * @param name     The name of the string.
 * @param dataType The datatype of the argument.
 * @param default  The default value of the argument. If the default is none, then that means the
 *                 argument is required. If no argument is provided, an exception is thrown.
 */
case class NamedArgument(
    name: String,
    dataType: NamedArgumentType,
    default: Option[Expression] = None)

/**
 * Represents a method signature and the list of arguments it receives as input.
 * Currently, overloads are not supported and only one FunctionSignature is allowed
 * per function expression.
 *
 * @param parameters The list of arguments which the function takes
 */
case class FunctionSignature(parameters: Seq[NamedArgument])
