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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._

/**
 * Validates that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator, and so are the ordering requirements.
 */
object ValidateRequirements extends Logging {

  def validate(plan: SparkPlan, requiredDistribution: Distribution): Boolean = {
    validate(plan) && plan.outputPartitioning.satisfies(requiredDistribution)
  }

  def validate(plan: SparkPlan): Boolean = {
    plan.children.forall(validate) && validateInternal(plan)
  }

  private def validateInternal(plan: SparkPlan): Boolean = {
    val children: Seq[SparkPlan] = plan.children
    val requiredChildDistributions: Seq[Distribution] = plan.requiredChildDistribution
    val requiredChildOrderings: Seq[Seq[SortOrder]] = plan.requiredChildOrdering
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    val satisfied = children.zip(requiredChildDistributions.zip(requiredChildOrderings)).forall {
      case (child, (distribution, ordering))
          if !child.outputPartitioning.satisfies(distribution)
            || !SortOrder.orderingSatisfies(child.outputOrdering, ordering) =>
        logDebug(s"ValidateRequirements failed: $distribution, $ordering\n$plan")
        false
      case _ => true
    }

    if (satisfied && children.length > 1 &&
      requiredChildDistributions.forall(_.isInstanceOf[ClusteredDistribution])) {
      // Check the co-partitioning requirement.
      val specs = children.map(_.outputPartitioning).zip(requiredChildDistributions).map {
        case (p, d) => p.createShuffleSpec(d.asInstanceOf[ClusteredDistribution])
      }
      if (specs.tail.forall(_.isCompatibleWith(specs.head))) {
        true
      } else {
        logDebug(s"ValidateRequirements failed: children not co-partitioned in\n$plan")
        false
      }
    } else {
      satisfied
    }
  }
}
