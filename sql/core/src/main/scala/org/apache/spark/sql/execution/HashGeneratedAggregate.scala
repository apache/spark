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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types._

/**
 * :: DeveloperApi ::
 * Alternate version of hash aggregation that leverages projection and thus code generation.
 * Aggregations are converted into a set of projections from a aggregation buffer tuple back onto
 * itself. Currently only used for simple aggregations like SUM, COUNT, or AVERAGE are supported.
 *
 * @param partial if true then aggregation is done partially on local data without shuffling to
 *                ensure all values where `groupingExpressions` are equal are present.
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param unsafeEnabled whether to allow Unsafe-based aggregation buffers to be used.
 * @param child the input data source.
 */
@DeveloperApi
case class HashGeneratedAggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    unsafeEnabled: Boolean,
    child: SparkPlan)
  extends UnaryNode with GeneratedAggregate {

  override def requiredChildDistribution: Seq[Distribution] =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  protected override def doExecute(): RDD[InternalRow] = {

    val aggregationBufferSchema: StructType = StructType.fromAttributes(computationSchema)

    val groupKeySchema: StructType = {
      val fields = groupingExpressions.zipWithIndex.map { case (expr, idx) =>
        // This is a dummy field name
        StructField(idx.toString, expr.dataType, expr.nullable)
      }
      StructType(fields)
    }

    val schemaSupportsUnsafe: Boolean = {
      UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema) &&
        UnsafeFixedWidthAggregationMap.supportsGroupKeySchema(groupKeySchema)
    }

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

      if (groupingExpressions.isEmpty) {
        // TODO: Codegening anything other than the updateProjection is probably over kill.
        val buffer = newAggregationBuffer(EmptyRow).asInstanceOf[MutableRow]
        var currentRow: InternalRow = null
        updateProjection.target(buffer)

        while (iter.hasNext) {
          currentRow = iter.next()
          updateProjection(joinedRow(buffer, currentRow))
        }

        val resultProjection = resultProjectionBuilder()
        Iterator(resultProjection(buffer))
      } else if (unsafeEnabled && schemaSupportsUnsafe) {
        log.info("Using Unsafe-based aggregator")
        val aggregationMap = new UnsafeFixedWidthAggregationMap(
          newAggregationBuffer(EmptyRow),
          aggregationBufferSchema,
          groupKeySchema,
          TaskContext.get.taskMemoryManager(),
          1024 * 16, // initial capacity
          false // disable tracking of performance metrics
        )

        while (iter.hasNext) {
          val currentRow: InternalRow = iter.next()
          val groupKey: InternalRow = groupProjection(currentRow)
          val aggregationBuffer = aggregationMap.getAggregationBuffer(groupKey)
          updateProjection.target(aggregationBuffer)(joinedRow(aggregationBuffer, currentRow))
        }

        new Iterator[InternalRow] {
          private[this] val mapIterator = aggregationMap.iterator()
          private[this] val resultProjection = resultProjectionBuilder()

          def hasNext: Boolean = mapIterator.hasNext

          def next(): InternalRow = {
            val entry = mapIterator.next()
            val result = resultProjection(joinedRow(entry.key, entry.value))
            if (hasNext) {
              result
            } else {
              // This is the last element in the iterator, so let's free the buffer. Before we do,
              // though, we need to make a defensive copy of the result so that we don't return an
              // object that might contain dangling pointers to the freed memory
              val resultCopy = result.copy()
              aggregationMap.free()
              resultCopy
            }
          }
        }
      } else {
        if (unsafeEnabled) {
          log.info("Not using Unsafe-based aggregator because it is not supported for this schema")
        }
        val buffers = new java.util.HashMap[InternalRow, MutableRow]()

        var currentRow: InternalRow = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupProjection(currentRow)
          var currentBuffer = buffers.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregationBuffer(EmptyRow).asInstanceOf[MutableRow]
            buffers.put(currentGroup, currentBuffer)
          }
          // Target the projection at the current aggregation buffer and then project the updated
          // values.
          updateProjection.target(currentBuffer)(joinedRow(currentBuffer, currentRow))
        }

        new Iterator[InternalRow] {
          private[this] val resultIterator = buffers.entrySet.iterator()
          private[this] val resultProjection = resultProjectionBuilder()

          def hasNext: Boolean = resultIterator.hasNext

          def next(): InternalRow = {
            val currentGroup = resultIterator.next()
            resultProjection(joinedRow(currentGroup.getKey, currentGroup.getValue))
          }
        }
      }
    }
  }
}
