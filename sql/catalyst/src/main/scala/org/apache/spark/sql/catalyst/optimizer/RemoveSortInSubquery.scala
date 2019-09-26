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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * [[Sort]] without [[Limit]] in subquery is useless. For example,
 *
 * {{{
 *   SELECT * FROM
 *    (SELECT f1 FROM tbl1 ORDER BY f2) temp1
 *   JOIN
 *    (SELECT f3 FROM tbl2) temp2
 *   ON temp1.f1 = temp2.f3
 * }}}
 *
 * is equal to
 *
 * {{{
 *  SELECT * FROM
 *   (SELECT f1 FROM tbl1) temp1
 *  JOIN
 *   (SELECT f3 FROM tbl2) temp2
 *  ON temp1.f1 = temp2.f3"
 * }}}
 *
 * This rule try to remove this kind of [[Sort]] operator.
 */
object RemoveSortInSubquery extends Rule[LogicalPlan] with PredicateHelper {
  private def removeTopLevelSort(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case Sort(_, _, child) => child
      case Project(fields, child) => Project(fields, removeTopLevelSort(child))
      case other => other
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case j @ Join(oldLeft, oldRight, _, _, _) =>
      j.copy(left = removeTopLevelSort(oldLeft), right = removeTopLevelSort(oldRight))
    case g @ Aggregate(_, _, oldChild) =>
      g.copy(child = removeTopLevelSort(oldChild))
  }
}
