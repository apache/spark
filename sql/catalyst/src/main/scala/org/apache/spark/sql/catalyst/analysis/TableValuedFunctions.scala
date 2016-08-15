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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Range}
import org.apache.spark.sql.catalyst.rules._

/**
 * Rule that resolves table-valued function references.
 */
object ResolveTableValuedFunctions extends Rule[LogicalPlan] {
  private lazy val defaultParallelism =
    SparkContext.getOrCreate(new SparkConf(false)).defaultParallelism

  /**
   * Type aliases for a TVF declaration. A TVF maps a sequence of named arguments to a function
   * resolving the TVF given a matching sequence of values from the user. This allows for
   * function overloading (e.g. range(100), range(0, 100)).
   */
  private type NamedArguments = Seq[Tuple2[String, Class[_]]]
  private type TVF = Map[NamedArguments, Seq[Any] => LogicalPlan]

  /**
   * Internal registry of table-valued-functions. TODO(ekl) we should have a proper registry
   */
  private val builtinFunctions: Map[String, TVF] = Map(
    "range" -> Map(
      /* range(end) */
      Seq(("end", classOf[Number])) -> (
        (args: Seq[Any]) =>
          Range(0, args(0).asInstanceOf[Number].longValue, 1, defaultParallelism)),

      /* range(start, end) */
      Seq(("start", classOf[Number]), ("end", classOf[Number])) -> (
        (args: Seq[Any]) =>
          Range(
            args(0).asInstanceOf[Number].longValue, args(1).asInstanceOf[Number].longValue, 1,
            defaultParallelism)),

      /* range(start, end, step) */
      Seq(("start", classOf[Number]), ("end", classOf[Number]), ("steps", classOf[Number])) -> (
        (args: Seq[Any]) =>
          Range(
            args(0).asInstanceOf[Number].longValue, args(1).asInstanceOf[Number].longValue,
            args(2).asInstanceOf[Number].longValue, defaultParallelism)),

      /* range(start, end, step, numPartitions) */
      Seq(("start", classOf[Number]), ("end", classOf[Number]), ("steps", classOf[Number]),
          ("numPartitions", classOf[Integer])) -> (
        (args: Seq[Any]) =>
          Range(
            args(0).asInstanceOf[Number].longValue, args(1).asInstanceOf[Number].longValue,
            args(2).asInstanceOf[Number].longValue, args(3).asInstanceOf[Integer]))
    )
  )

  /**
   * Returns whether a given sequence of values can be assigned to the specified arguments.
   */
  private def assignableFrom(args: NamedArguments, values: Seq[Any]): Boolean = {
    if (args.length == values.length) {
      args.zip(values).forall { case ((name, clazz), value) =>
        clazz.isAssignableFrom(value.getClass)
      }
    } else {
      false
    }
  }

  /**
   * Formats a list of named args, e.g. to "start: Number, end: Number, steps: Number".
   */
  private def formatArgs(args: NamedArguments): String = {
    args.map { a =>
      s"${a._1}: ${a._2.getSimpleName}"
    }.mkString(", ")
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UnresolvedTableValuedFunction =>
      builtinFunctions.get(u.functionName) match {
        case Some(tvf) =>
          val evaluatedArgs = u.functionArgs.map(_.eval())
          for ((argSpec, resolver) <- tvf) {
            if (assignableFrom(argSpec, evaluatedArgs)) {
              return resolver(evaluatedArgs)
            }
          }
          val argTypes = evaluatedArgs.map(_.getClass.getSimpleName).mkString(", ")
          u.failAnalysis(
            s"""error: table-valued function ${u.functionName} with alternatives:
              |${tvf.keys.map(formatArgs).toSeq.sorted.map(x => s" ($x)").mkString("\n")}
              |cannot be applied to: (${argTypes})""".stripMargin)
        case _ =>
          u.failAnalysis(s"could not resolve `${u.functionName}` to a table valued function")
      }
  }
}
