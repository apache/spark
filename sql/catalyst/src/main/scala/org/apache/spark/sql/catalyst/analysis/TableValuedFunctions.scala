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
   * List of argument names and their types, used to declare a function call.
   */
  private case class ArgumentList(args: (String, Class[_])*) {
    /**
     * @return whether this list is assignable from the given sequence of values.
     */
    def assignableFrom(values: Seq[Any]): Boolean = {
      if (args.length == values.length) {
        args.zip(values).forall { case ((name, clazz), value) =>
          clazz.isAssignableFrom(value.getClass)
        }
      } else {
        false
      }
    }

    override def toString: String = {
      args.map { a =>
        s"${a._1}: ${a._2.getSimpleName}"
      }.mkString(", ")
    }
  }

  /**
   * A TVF maps argument lists to a resolving functions that accept those arguments. Using a map
   * here allows for function overloading.
   */
  private type TVF = Map[ArgumentList, Seq[Any] => LogicalPlan]

  /**
   * Internal registry of table-valued-functions. TODO(ekl) we should have a proper registry
   */
  private val builtinFunctions: Map[String, TVF] = Map(
    "range" -> Map(
      /* range(end) */
      ArgumentList(("end", classOf[Number])) -> (
        (args: Seq[Any]) =>
          Range(0, args(0).asInstanceOf[Number].longValue, 1, defaultParallelism)),

      /* range(start, end) */
      ArgumentList(("start", classOf[Number]), ("end", classOf[Number])) -> (
        (args: Seq[Any]) =>
          Range(
            args(0).asInstanceOf[Number].longValue, args(1).asInstanceOf[Number].longValue, 1,
            defaultParallelism)),

      /* range(start, end, step) */
      ArgumentList(("start", classOf[Number]), ("end", classOf[Number]),
          ("steps", classOf[Number])) -> (
        (args: Seq[Any]) =>
          Range(
            args(0).asInstanceOf[Number].longValue, args(1).asInstanceOf[Number].longValue,
            args(2).asInstanceOf[Number].longValue, defaultParallelism)),

      /* range(start, end, step, numPartitions) */
      ArgumentList(("start", classOf[Number]), ("end", classOf[Number]),
          ("steps", classOf[Number]), ("numPartitions", classOf[Integer])) -> (
        (args: Seq[Any]) =>
          Range(
            args(0).asInstanceOf[Number].longValue, args(1).asInstanceOf[Number].longValue,
            args(2).asInstanceOf[Number].longValue, args(3).asInstanceOf[Integer])))
  )

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UnresolvedTableValuedFunction =>
      builtinFunctions.get(u.functionName) match {
        case Some(tvf) =>
          val evaluatedArgs = u.functionArgs.map(_.eval())
          for ((argList, resolver) <- tvf) {
            if (argList.assignableFrom(evaluatedArgs)) {
              return resolver(evaluatedArgs)
            }
          }
          val argTypes = evaluatedArgs.map(_.getClass.getSimpleName).mkString(", ")
          u.failAnalysis(
            s"""error: table-valued function ${u.functionName} with alternatives:
              |${tvf.keys.map(_.toString).toSeq.sorted.map(x => s" ($x)").mkString("\n")}
              |cannot be applied to: (${argTypes})""".stripMargin)
        case _ =>
          u.failAnalysis(s"could not resolve `${u.functionName}` to a table valued function")
      }
  }
}
