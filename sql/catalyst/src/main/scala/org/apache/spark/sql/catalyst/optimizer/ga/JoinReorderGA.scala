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

package org.apache.spark.sql.catalyst.optimizer.ga

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.Cost
import org.apache.spark.sql.catalyst.optimizer.JoinReorderDP.{sameOutput, JoinPlan}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf


object JoinReorderGA extends PredicateHelper with Logging {

  def search(
      conf: SQLConf,
      items: Seq[LogicalPlan],
      conditions: Set[Expression],
      output: Seq[Attribute]): Option[LogicalPlan] = {

    val startTime = System.nanoTime()

    val itemsWithIndex = items.zipWithIndex.map {
      case (plan, id) => id -> JoinPlan(Set(id), plan, Set.empty, Cost(0, 0))
    }.toMap

    val topOutputSet = AttributeSet(output)

    val pop = Population(conf, itemsWithIndex, conditions, topOutputSet).evolve

    val durationInMs = (System.nanoTime() - startTime) / (1000 * 1000)
    logInfo(s"Join reordering finished. Duration: $durationInMs ms, number of items: " +
        s"${items.length}, number of plans in memo: ${ pop.chromos.size}")

    assert(pop.chromos.head.basicPlans.size == items.length)
    pop.chromos.head.integratedPlan match {
      case Some(joinPlan) => joinPlan.plan match {
        case p @ Project(projectList, _: Join) if projectList != output =>
          assert(topOutputSet == p.outputSet)
          // Keep the same order of final output attributes.
          Some(p.copy(projectList = output))
        case finalPlan if !sameOutput(finalPlan, output) =>
          Some(Project(output, finalPlan))
        case finalPlan =>
          Some(finalPlan)
      }
      case _ => None
    }
  }
}
