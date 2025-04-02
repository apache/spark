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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.streaming.StatefulOperatorPartitioning

/**
 * This node updates the session window spec of each input rows via analyzing neighbor rows and
 * determining rows belong to the same session window. The number of input rows remains the same.
 * This node requires sort on input rows by group keys + the start time of session window.
 *
 * There are lots of overhead compared to [[MergingSessionsExec]]. Use [[MergingSessionsExec]]
 * instead whenever possible. Use this node only when we cannot apply both calculations
 * determining session windows and aggregating rows in session window altogether.
 *
 * Refer [[UpdatingSessionsIterator]] for more details.
 */
case class UpdatingSessionsExec(
    isStreaming: Boolean,
    numShufflePartitions: Option[Int],
    groupingExpression: Seq[Attribute],
    sessionExpression: Attribute,
    child: SparkPlan) extends UnaryExecNode {

  private val groupingWithoutSessionExpression = groupingExpression.filterNot {
    p => p.semanticEquals(sessionExpression)
  }
  private val groupingWithoutSessionAttributes =
    groupingWithoutSessionExpression.map(_.toAttribute)

  override protected def doExecute(): RDD[InternalRow] = {
    val inMemoryThreshold = conf.sessionWindowBufferInMemoryThreshold
    val spillThreshold = conf.sessionWindowBufferSpillThreshold

    child.execute().mapPartitions { iter =>
      new UpdatingSessionsIterator(iter, groupingExpression, sessionExpression,
        child.output, inMemoryThreshold, spillThreshold)
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    if (groupingWithoutSessionExpression.isEmpty) {
      AllTuples :: Nil
    } else {
      if (isStreaming) {
        numShufflePartitions match {
          case Some(parts) =>
            StatefulOperatorPartitioning.getCompatibleDistribution(
              groupingWithoutSessionExpression, parts, conf) :: Nil

          case _ =>
            throw SparkException.internalError("Expected to set the number of partitions before " +
              "constructing required child distribution!")
        }

      } else {
        ClusteredDistribution(groupingWithoutSessionExpression) :: Nil
      }
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    Seq((groupingWithoutSessionAttributes ++ Seq(sessionExpression))
      .map(SortOrder(_, Ascending)))
  }

  override protected def withNewChildInternal(newChild: SparkPlan): UpdatingSessionsExec =
    copy(child = newChild)
}
