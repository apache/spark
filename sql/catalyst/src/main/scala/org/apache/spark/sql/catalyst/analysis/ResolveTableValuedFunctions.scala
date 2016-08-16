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
  private lazy val defaultParallelism =
    SparkContext.getOrCreate(new SparkConf(false)).defaultParallelism

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
   * Internal registry of table-valued functions. TODO(ekl) we should have a proper registry
   */
  private val builtinFunctions: Map[String, TVF] = Map(
    "range" -> Map(
      /* range(end) */
      ArgumentList(("end", LongType)) -> (
        (args: Seq[Any]) =>
          Range(0, args(0).asInstanceOf[Long], 1, defaultParallelism)),

      /* range(start, end) */
      ArgumentList(("start", LongType), ("end", LongType)) -> (
        (args: Seq[Any]) =>
          Range(
            args(0).asInstanceOf[Long], args(1).asInstanceOf[Long], 1, defaultParallelism)),

      /* range(start, end, step) */
      ArgumentList(("start", LongType), ("end", LongType), ("step", LongType)) -> (
        (args: Seq[Any]) =>
          Range(
            args(0).asInstanceOf[Long], args(1).asInstanceOf[Long], args(2).asInstanceOf[Long],
            defaultParallelism)),

      /* range(start, end, step, numPartitions) */
      ArgumentList(("start", LongType), ("end", LongType), ("step", LongType),
          ("numPartitions", IntegerType)) -> (
        (args: Seq[Any]) =>
          Range(
            args(0).asInstanceOf[Long], args(1).asInstanceOf[Long], args(2).asInstanceOf[Long],
            args(3).asInstanceOf[Integer])))
  )

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UnresolvedTableValuedFunction =>
      builtinFunctions.get(u.functionName) match {
        case Some(tvf) =>
          for ((argList, resolver) <- tvf) {
            val casted = argList.implicitCast(u.functionArgs)
            if (casted.isDefined) {
              return resolver(casted.get.map(_.eval()))
            }
          }
          val argTypes = u.functionArgs.map(_.dataType.typeName).mkString(", ")
          u.failAnalysis(
            s"""error: table-valued function ${u.functionName} with alternatives:
              |${tvf.keys.map(_.toString).toSeq.sorted.map(x => s" ($x)").mkString("\n")}
              |cannot be applied to: (${argTypes})""".stripMargin)
        case _ =>
          u.failAnalysis(s"could not resolve `${u.functionName}` to a table-valued function")
      }
  }
}
