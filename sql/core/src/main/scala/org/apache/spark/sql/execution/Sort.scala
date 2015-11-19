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

import org.apache.spark.{InternalAccumulator, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Performs (external) sorting.
 *
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 * @param testSpillFrequency Method for configuring periodic spilling in unit tests. If set, will
 *                           spill every `frequency` records.
 */
case class Sort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    testSpillFrequency: Int = 0)
  extends UnaryNode {

  override def outputsUnsafeRows: Boolean = true
  override def canProcessUnsafeRows: Boolean = true
  override def canProcessSafeRows: Boolean = false

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override private[sql] lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  protected override def doExecute(): RDD[InternalRow] = {
    val schema = child.schema
    val childOutput = child.output

    val dataSize = longMetric("dataSize")
    val spillSize = longMetric("spillSize")

    child.execute().mapPartitionsInternal { iter =>
      val ordering = newOrdering(sortOrder, childOutput)

      // The comparator for comparing prefix
      val boundSortExpression = BindReferences.bindReference(sortOrder.head, childOutput)
      val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

      // The generator for prefix
      val prefixProjection = UnsafeProjection.create(Seq(SortPrefix(boundSortExpression)))
      val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
        override def computePrefix(row: InternalRow): Long = {
          prefixProjection.apply(row).getLong(0)
        }
      }

      val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
      val sorter = new UnsafeExternalRowSorter(
        schema, ordering, prefixComparator, prefixComputer, pageSize)
      if (testSpillFrequency > 0) {
        sorter.setTestSpillFrequency(testSpillFrequency)
      }

      // Remember spill data size of this task before execute this operator so that we can
      // figure out how many bytes we spilled for this operator.
      val spillSizeBefore = TaskContext.get().taskMetrics().memoryBytesSpilled

      val sortedIterator = sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])

      dataSize += sorter.getPeakMemoryUsage
      spillSize += TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore

      TaskContext.get().internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(sorter.getPeakMemoryUsage)
      sortedIterator
    }
  }
}
