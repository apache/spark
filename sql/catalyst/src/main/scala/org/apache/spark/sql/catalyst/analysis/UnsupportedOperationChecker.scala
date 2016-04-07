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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Analyzes the presence of unsupported operations in a logical plan.
 */
trait UnsupportedOperationChecker {
  def checkUnsupportedOperations(logicalPlan: LogicalPlan, forIncremental: Boolean): Unit = {
    if (forIncremental) {
      checkUnsupportedOperationsForIncremental(logicalPlan)
    } else {
      logicalPlan.foreachUp {
        case p if p.needsIncrementalExcecution =>
          notSupported(
            logicalPlan,
            "Queries with a streaming source must by executed with write.startStream()")

        case _ =>
      }
    }
  }

  private def checkUnsupportedOperationsForIncremental(logicalPlan: LogicalPlan): Unit = {
    logicalPlan.foreachUp {

      case j@Join(left, right, _, _)
        if (left.needsIncrementalExcecution && right.needsIncrementalExcecution) =>
        notSupported(j, "Stream-stream joins are not supported in ContinuousQueries")

      case UnsupportedForIncrementalExecution(op) if op.needsIncrementalExcecution =>
        notSupportedForIncremental(op)

      case _ =>
    }
  }

  private def notSupportedForIncremental(operator: LogicalPlan): Nothing = {
    notSupported(
      operator, s"${operator.getClass.getSimpleName} is not supported in ContinuousQueries")
  }

  private def notSupported(operator: LogicalPlan, msg: String): Nothing = {
    throw new AnalysisException(
      msg, operator.origin.line, operator.origin.startPosition, Some(operator))
  }

  object UnsupportedForIncrementalExecution {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
      case _: Command =>
        Some(plan)

      case
        GlobalLimit(_, _) |
        LocalLimit(_, _) |
        Distinct(_) |
        Sort(_, _, _) =>

        Some(plan)

      case _ =>
        None
    }
  }
}
