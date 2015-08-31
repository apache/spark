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

import org.apache.spark.rdd.{MapPartitionsWithPreparationRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter
import org.apache.spark.{SparkEnv, InternalAccumulator, TaskContext}

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines various sort operators.
////////////////////////////////////////////////////////////////////////////////////////////////////


/**
 * Performs a sort on-heap.
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 */
case class Sort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryNode {
  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "sort") {
    child.execute().mapPartitions( { iterator =>
      val ordering = newOrdering(sortOrder, child.output)
      iterator.map(_.copy()).toArray.sorted(ordering).iterator
    }, preservesPartitioning = true)
  }

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder
}

/**
 * Performs a sort, spilling to disk as needed.
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 */
case class ExternalSort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryNode {

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "sort") {
    child.execute().mapPartitions( { iterator =>
      val ordering = newOrdering(sortOrder, child.output)
      val sorter = new ExternalSorter[InternalRow, Null, InternalRow](ordering = Some(ordering))
      sorter.insertAll(iterator.map(r => (r.copy(), null)))
      val baseIterator = sorter.iterator.map(_._1)
      val context = TaskContext.get()
      context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
      context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
      context.internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(sorter.peakMemoryUsedBytes)
      // TODO(marmbrus): The complex type signature below thwarts inference for no reason.
      CompletionIterator[InternalRow, Iterator[InternalRow]](baseIterator, sorter.stop())
    }, preservesPartitioning = true)
  }

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder
}

/**
 * Optimized version of [[ExternalSort]] that operates on binary data (implemented as part of
 * Project Tungsten).
 *
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 * @param testSpillFrequency Method for configuring periodic spilling in unit tests. If set, will
 *                           spill every `frequency` records.
 */
case class TungstenSort(
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

  protected override def doExecute(): RDD[InternalRow] = {
    val schema = child.schema
    val childOutput = child.output

    /**
     * Set up the sorter in each partition before computing the parent partition.
     * This makes sure our sorter is not starved by other sorters used in the same task.
     */
    def preparePartition(): UnsafeExternalRowSorter = {
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

      val pageSize = SparkEnv.get.shuffleMemoryManager.pageSizeBytes
      val sorter = new UnsafeExternalRowSorter(
        schema, ordering, prefixComparator, prefixComputer, pageSize)
      if (testSpillFrequency > 0) {
        sorter.setTestSpillFrequency(testSpillFrequency)
      }
      sorter
    }

    /** Compute a partition using the sorter already set up previously. */
    def executePartition(
        taskContext: TaskContext,
        partitionIndex: Int,
        sorter: UnsafeExternalRowSorter,
        parentIterator: Iterator[InternalRow]): Iterator[InternalRow] = {
      val sortedIterator = sorter.sort(parentIterator.asInstanceOf[Iterator[UnsafeRow]])
      taskContext.internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(sorter.getPeakMemoryUsage)
      sortedIterator
    }

    // Note: we need to set up the external sorter in each partition before computing
    // the parent partition, so we cannot simply use `mapPartitions` here (SPARK-9709).
    new MapPartitionsWithPreparationRDD[InternalRow, InternalRow, UnsafeExternalRowSorter](
      child.execute(), preparePartition, executePartition, preservesPartitioning = true)
  }

}

object TungstenSort {
  /**
   * Return true if UnsafeExternalSort can sort rows with the given schema, false otherwise.
   */
  def supportsSchema(schema: StructType): Boolean = {
    UnsafeExternalRowSorter.supportsSchema(schema)
  }
}
