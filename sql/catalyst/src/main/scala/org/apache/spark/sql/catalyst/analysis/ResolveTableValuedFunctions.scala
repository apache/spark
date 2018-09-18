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

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Range}
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

/**
 * Rule that resolves table-valued function references.
 */
object ResolveTableValuedFunctions extends Rule[LogicalPlan] {
  /**
   * List of argument names and their types, used to declare a function.
   */
  private case class ArgumentList(args: (String, DataType)*) {
    /**
     * Try to cast the expressions to satisfy the expected types of this argument list. If there
     * are any types that cannot be casted, then None is returned.
     */
    def implicitCast(values: Seq[Expression]): Option[Seq[Expression]] = {
      if (args.length == values.length) {
        val casted = values.zip(args).map { case (value, (_, expectedType)) =>
          TypeCoercion.ImplicitTypeCasts.implicitCast(value, expectedType)
        }
        if (casted.forall(_.isDefined)) {
          return Some(casted.map(_.get))
        }
      }
      None
    }

    override def toString: String = {
      args.map { a =>
        s"${a._1}: ${a._2.typeName}"
      }.mkString(", ")
    }
  }

  /**
   * A TVF maps argument lists to resolver functions that accept those arguments. Using a map
   * here allows for function overloading.
   */
  private type TVF = Map[ArgumentList, Seq[Any] => LogicalPlan]

  /**
   * TVF builder.
   */
  private def tvf(args: (String, DataType)*)(pf: PartialFunction[Seq[Any], LogicalPlan])
      : (ArgumentList, Seq[Any] => LogicalPlan) = {
    (ArgumentList(args: _*),
     pf orElse {
       case arguments =>
         // This is caught again by the apply function and rethrow with richer information about
         // position, etc, for a better error message.
         throw new AnalysisException(
           "Invalid arguments for resolved function: " + arguments.mkString(", "))
     })
  }

  /**
   * Internal registry of table-valued functions.
   */
  private val builtinFunctions: Map[String, TVF] = Map(
    "range" -> Map(
      /* range(end) */
      tvf("end" -> LongType) { case Seq(end: Long) =>
        Range(0, end, 1, None)
      },

      /* range(start, end) */
      tvf("start" -> LongType, "end" -> LongType) { case Seq(start: Long, end: Long) =>
        Range(start, end, 1, None)
      },

      /* range(start, end, step) */
      tvf("start" -> LongType, "end" -> LongType, "step" -> LongType) {
        case Seq(start: Long, end: Long, step: Long) =>
          Range(start, end, step, None)
      },

      /* range(start, end, step, numPartitions) */
      tvf("start" -> LongType, "end" -> LongType, "step" -> LongType,
          "numPartitions" -> IntegerType) {
        case Seq(start: Long, end: Long, step: Long, numPartitions: Int) =>
          Range(start, end, step, Some(numPartitions))
      })
  )

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UnresolvedTableValuedFunction if u.functionArgs.forall(_.resolved) =>
      // The whole resolution is somewhat difficult to understand here due to too much abstractions.
      // We should probably rewrite the following at some point. Reynold was just here to improve
      // error messages and didn't have time to do a proper rewrite.
      val resolvedFunc = builtinFunctions.get(u.functionName.toLowerCase(Locale.ROOT)) match {
        case Some(tvf) =>

          def failAnalysis(): Nothing = {
            val argTypes = u.functionArgs.map(_.dataType.typeName).mkString(", ")
            u.failAnalysis(
              s"""error: table-valued function ${u.functionName} with alternatives:
                 |${tvf.keys.map(_.toString).toSeq.sorted.map(x => s" ($x)").mkString("\n")}
                 |cannot be applied to: ($argTypes)""".stripMargin)
          }

          val resolved = tvf.flatMap { case (argList, resolver) =>
            argList.implicitCast(u.functionArgs) match {
              case Some(casted) =>
                try {
                  Some(resolver(casted.map(_.eval())))
                } catch {
                  case e: AnalysisException =>
                    failAnalysis()
                }
              case _ =>
                None
            }
          }
          resolved.headOption.getOrElse {
            failAnalysis()
          }
        case _ =>
          u.failAnalysis(s"could not resolve `${u.functionName}` to a table-valued function")
      }

      // If alias names assigned, add `Project` with the aliases
      if (u.outputNames.nonEmpty) {
        val outputAttrs = resolvedFunc.output
        // Checks if the number of the aliases is equal to expected one
        if (u.outputNames.size != outputAttrs.size) {
          u.failAnalysis(s"Number of given aliases does not match number of output columns. " +
            s"Function name: ${u.functionName}; number of aliases: " +
            s"${u.outputNames.size}; number of output columns: ${outputAttrs.size}.")
        }
        val aliases = outputAttrs.zip(u.outputNames).map {
          case (attr, name) => Alias(attr, name)()
        }
        Project(aliases, resolvedFunc)
      } else {
        resolvedFunc
      }
  }
}
