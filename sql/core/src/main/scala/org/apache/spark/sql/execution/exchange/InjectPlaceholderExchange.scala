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

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

case class InjectPlaceholderExchange(conf: SQLConf) extends Rule[SparkPlan] {
  private def defaultNumPreShufflePartitions: Int = conf.numShufflePartitions

  /**
   * Given a required distribution, returns a partitioning that satisfies that distribution.
   * @param requiredDistribution The distribution that is required by the operator
   * @param numPartitions Used when the distribution doesn't require a specific number of partitions
   */
  private def createPartitioning(requiredDistribution: Distribution,
                                 numPartitions: Int): Partitioning = {
    requiredDistribution match {
      case AllTuples => SinglePartition
      case ClusteredDistribution(clustering, desiredPartitions) =>
        HashPartitioning(clustering, desiredPartitions.getOrElse(numPartitions))
      case OrderedDistribution(ordering) => RangePartitioning(ordering, numPartitions)
      case dist => sys.error(s"Do not know how to satisfy distribution $dist")
    }
  }

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator @ ShuffleExchangeExec(partitioning, child, _) =>
      child.children match {
        case ShuffleExchangeExec(childPartitioning, baseChild, _)::Nil =>
          if (childPartitioning.guarantees(partitioning)) child else operator
        case _ => operator
      }

    case operator: SparkPlan =>
      val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
      val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
      var children: Seq[SparkPlan] = operator.children

      children = children.zip(requiredChildDistributions).map {
        case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
          child
        case (child, BroadcastDistribution(mode)) =>
          BroadcastExchangeExec(mode, child)
        case (child, distribution) =>
          ShuffleExchangeExec(
            createPartitioning(distribution, defaultNumPreShufflePartitions), child)
      }
      operator.withNewChildren(children)
  }
}
