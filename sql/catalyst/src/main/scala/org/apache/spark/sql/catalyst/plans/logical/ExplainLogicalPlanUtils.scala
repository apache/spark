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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan

object ExplainLogicalPlanUtils extends BaseExplainUtils[LogicalPlan] {

  override protected def generateOperatorIDs(
      plan: QueryPlan[_],
      startOperatorID: Int,
      operatorIDs: mutable.ArrayBuffer[(Int, QueryPlan[_])]): Int = {
    var currentOperationID = startOperatorID
    plan.foreachUp { case plan: QueryPlan[_] =>
      def setOpId(): Unit = if (plan.getTagValue(QueryPlan.OP_ID_TAG).isEmpty) {
        currentOperationID += 1
        plan.setTagValue(QueryPlan.OP_ID_TAG, currentOperationID)
        operatorIDs += ((currentOperationID, plan))
      }
      setOpId()

      // Skip the subqueries as they are not printed as part of main query block.
      val subquries = plan.subqueries.map(_.canonicalized)
      val nonSubquries = plan.innerChildren.filterNot { p =>
        subquries.exists(_ == p.canonicalized)
      }
      nonSubquries.foldLeft(currentOperationID) {
        (curId, ip) => generateOperatorIDs(ip, curId, operatorIDs)
      }
    }
    currentOperationID
  }

  override def getSubqueries(
      plan: => LogicalPlan,
      subqueries: ArrayBuffer[(LogicalPlan, Expression, LogicalPlan)]): Unit = {
    plan.foreach { p =>
      p.expressions.foreach (_.collect {
        case e: SubqueryExpression =>
          subqueries += ((p, e, e.plan))
          getSubqueries(e.plan, subqueries)
      })
    }
  }
}
