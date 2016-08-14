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

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Range}
import org.apache.spark.sql.catalyst.rules._

/**
 * Rule for resolving references to table-valued functions. Currently this only resolves
 * references to the hard-coded range() operator.
 */
object ResolveTableValuedFunctions extends Rule[LogicalPlan] {
  private def defaultParallelism: Int = 200  // TODO(ekl) fix

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UnresolvedTableValuedFunction =>
      // TODO(ekl) we should have a tvf registry
      if (u.functionName != "range") {
        u.failAnalysis(s"could not resolve `${u.functionName}` to a table valued function")
      }
      val evaluatedArgs = u.functionArgs.map(_.eval())
      val longArgs = evaluatedArgs.map(_.toString.toLong)   // TODO(ekl) fix
      longArgs match {
        case Seq(end) =>
          Range(0, end, 1, defaultParallelism)
        case Seq(start, end) =>
          Range(start, end, 1, defaultParallelism)
        case Seq(start, end, step) =>
          Range(start, end, step, defaultParallelism)
        case Seq(start, end, step, numPartitions) =>
          Range(start, end, step, numPartitions.toInt)
        case _ =>
          u.failAnalysis(s"invalid number of argument for range(): ${longArgs}")
      }
  }
}
