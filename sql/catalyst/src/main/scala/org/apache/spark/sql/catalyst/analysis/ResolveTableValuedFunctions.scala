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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Range}
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
       case args =>
         throw new IllegalArgumentException(
           "Invalid arguments for resolved function: " + args.mkString(", "))
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
      builtinFunctions.get(u.functionName) match {
        case Some(tvf) =>
          val resolved = tvf.flatMap { case (argList, resolver) =>
            argList.implicitCast(u.functionArgs) match {
              case Some(casted) =>
                Some(resolver(casted.map(_.eval())))
              case _ =>
                None
            }
          }
          resolved.headOption.getOrElse {
            val argTypes = u.functionArgs.map(_.dataType.typeName).mkString(", ")
            u.failAnalysis(
              s"""error: table-valued function ${u.functionName} with alternatives:
                |${tvf.keys.map(_.toString).toSeq.sorted.map(x => s" ($x)").mkString("\n")}
                |cannot be applied to: (${argTypes})""".stripMargin)
          }
        case _ =>
          u.failAnalysis(s"could not resolve `${u.functionName}` to a table-valued function")
      }
  }
}
