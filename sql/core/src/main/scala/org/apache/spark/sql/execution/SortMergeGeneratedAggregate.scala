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

package org.apache.spark.sql.execution

import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.trees._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types._

/**
 * :: DeveloperApi ::
 * Alternate version of sort merge aggregation that leverages projection and thus code generation.
 * Aggregations are converted into a set of projections from a aggregation buffer tuple back onto
 * itself. Currently only used for simple aggregations like SUM, COUNT, or AVERAGE are supported.
 *
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param unsafeEnabled whether to allow Unsafe-based aggregation buffers to be used.
 * @param child the input data source.
 */
@DeveloperApi
case class SortMergeGeneratedAggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    unsafeEnabled: Boolean,
    sortEnabled: Boolean,
    child: SparkPlan)
  extends UnaryNode with GeneratedAggregate {

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingExpressions) :: Nil

  // this is to manually construct an ordering that can be used to compare keys
  private val keyOrdering: RowOrdering = RowOrdering.forSchema(groupingExpressions.map(_.dataType))

  override def outputOrdering: Seq[SortOrder] = requiredOrders(groupingExpressions)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(groupingExpressions) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] =
    groupingExpressions.map(SortOrder(_, Ascending))

  protected override def doExecute(): RDD[InternalRow] = {

    child.execute().mapPartitions { iter =>

      // Builds a new custom class for holding the results of aggregation for a group.
      val initialValues = computeFunctions.flatMap(_.initialValues)
      val newAggregationBuffer = newProjection(initialValues, child.output)
      log.info(s"Initial values: ${initialValues.mkString(",")}")

      // A projection that computes the group given an input tuple.
      val groupProjection = newProjection(groupingExpressions, child.output)
      log.info(s"Grouping Projection: ${groupingExpressions.mkString(",")}")

      // A projection that is used to update the aggregate values for a group given a new tuple.
      // This projection should be targeted at the current values for the group and then applied
      // to a joined row of the current values with the new input row.
      val updateExpressions = computeFunctions.flatMap(_.update)
      val updateSchema = computeFunctions.flatMap(_.schema) ++ child.output
      val updateProjection = newMutableProjection(updateExpressions, updateSchema)()
      log.info(s"Update Expressions: ${updateExpressions.mkString(",")}")

      // A projection that produces the final result, given a computation.
      val resultProjectionBuilder =
        newMutableProjection(
          resultExpressions,
          namedGroups.map(_._2) ++ computationSchema)
      log.info(s"Result Projection: ${resultExpressions.mkString(",")}")

      val joinedRow = new JoinedRow3

      new Iterator[InternalRow] {

        private[this] var currentElement: InternalRow = _
        private[this] var nextElement: InternalRow = _
        private[this] var currentKey: InternalRow = _
        private[this] var nextKey: InternalRow = _
        private[this] var currentBuffer: MutableRow = _
        private[this] val resultProjection = resultProjectionBuilder()

        initialize()

        private def initialize() = {
          if (iter.hasNext) {
            currentElement = iter.next()
            currentKey = groupProjection(currentElement)
          } else {
            currentElement = null
          }
        }

        override final def hasNext: Boolean = {
          if (currentElement != null) {
            currentBuffer = newAggregationBuffer(EmptyRow).asInstanceOf[MutableRow]
            // Target the projection at the current aggregation buffer and then project the updated
            // values.
            updateProjection.target(currentBuffer)(joinedRow(currentBuffer, currentElement))
            var stop: Boolean = false
            while (!stop) {
              if (iter.hasNext) {
                nextElement = iter.next()
                nextKey = groupProjection(nextElement)
                stop = keyOrdering.compare(currentKey, nextKey) != 0
                if (!stop) {
                  updateProjection.target(currentBuffer)(joinedRow(currentBuffer, nextElement))
                }
              } else {
                nextElement = null
                stop = true
              }
            }
            true
          } else {
            false
          }
        }

        override final def next(): InternalRow = {
          val currentGroup = currentKey
          currentElement = nextElement
          currentKey = nextKey
          resultProjection(joinedRow(currentGroup, currentBuffer))
        }
      }
    }
  }
}
