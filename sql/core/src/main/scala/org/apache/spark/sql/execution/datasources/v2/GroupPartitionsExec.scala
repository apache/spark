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


package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Partition, SparkException}
import org.apache.spark.rdd.{CoalescedRDD, PartitionCoalescer, PartitionGroup, RDD, SortedMergeCoalescedRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.util.{truncatedString, InternalRowComparableWrapper}
import org.apache.spark.sql.connector.catalog.functions.Reducer
import org.apache.spark.sql.execution.{SafeForKWayMerge, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Physical operator that groups input partitions by their partition keys.
 *
 * This operator is used to coalesce partitions from bucketed/partitioned data sources
 * where multiple input partitions share the same partition key. It's commonly used in
 * storage-partitioned joins to align partitions from different sides of the join.
 *
 * @param child The child plan providing bucketed/partitioned input
 * @param joinKeyPositions Optional projection to select a subset of the partitioning key
 *                         for join compatibility (e.g., when join keys are a subset of
 *                         partition keys)
 * @param expectedPartitionKeys Optional sequence of expected partition key values and their
 *                              split counts
 * @param reducers Optional reducers to apply to partition keys for grouping compatibility
 * @param distributePartitions When true, splits for a key are distributed across the expected
 *                             partitions (padding with empty partitions). When false, all splits
 *                             are replicated to every expected partition for that key.
 */
case class GroupPartitionsExec(
    child: SparkPlan,
    @transient joinKeyPositions: Option[Seq[Int]] = None,
    @transient expectedPartitionKeys: Option[Seq[(InternalRowComparableWrapper, Int)]] = None,
    @transient reducers: Option[Seq[Option[Reducer[_, _]]]] = None,
    @transient distributePartitions: Boolean = false
  ) extends UnaryExecNode {

  override def outputPartitioning: Partitioning = {
    child.outputPartitioning match {
      case p: Partitioning with Expression =>
        // There can be multiple `KeyedPartitioning` in an output partitioning of a join, but they
        // can only differ in `expressions`. `partitionKeys` must match so we can calculate it only
        // once via `groupedPartitions`.

        val keyedPartitionings = p.collect { case k: KeyedPartitioning => k }
        if (keyedPartitionings.size > 1) {
          val first = keyedPartitionings.head
          keyedPartitionings.tail.foreach { k =>
            assert(k.partitionKeys == first.partitionKeys,
              "All KeyedPartitioning nodes must have identical partition keys")
          }
        }

        p.transform {
          case k: KeyedPartitioning =>
            val projectedExpressions = joinKeyPositions.fold(k.expressions)(_.map(k.expressions))
            KeyedPartitioning(projectedExpressions, groupedPartitions.map(_._1),
              isGrouped = isGrouped)
        }.asInstanceOf[Partitioning]
      case o => o
    }
  }

  /**
   * Aligns partitions based on `expectedPartitionKeys` and clustering mode.
   */
  private def alignToExpectedKeys(keyMap: Map[InternalRowComparableWrapper, Seq[Int]]) = {
    var isGrouped = true
    val alignedPartitions = expectedPartitionKeys.get.flatMap { case (key, numSplits) =>
      if (numSplits > 1) isGrouped = false
      val splits = keyMap.getOrElse(key, Seq.empty)
      if (distributePartitions) {
        // Distribute splits across expected partitions, padding with empty sequences
        val paddedSplits = splits.map(Seq(_)).padTo(numSplits, Seq.empty)
        paddedSplits.map((key, _))
      } else {
        // Replicate all splits to each expected partition
        Seq.fill(numSplits)((key, splits))
      }
    }
    (alignedPartitions, isGrouped)
  }

  /**
   * Groups and sorts partitions by their keys in ascending order.
   */
  private def groupAndSortByKeys(
      keyMap: Map[InternalRowComparableWrapper, Seq[Int]],
      dataTypes: Seq[DataType]) = {
    val keyOrdering = RowOrdering.createNaturalAscendingOrdering(dataTypes)
    keyMap.toSeq.sorted(keyOrdering.on((t: (InternalRowComparableWrapper, _)) => t._1.row))
  }

  /**
   * Computes the grouped partitions by:
   * 1. Projecting partition keys if joinKeyPositions is specified
   * 2. Reducing keys if reducers are specified
   * 3. Grouping input partition indices by their (possibly projected/reduced) keys
   * 4. Sorting or distributing based on whether partial clustering is enabled
   *
   * Returns a tuple of (partitions, isGrouped) where:
   * - partitions: sequence of (partitionKey, inputPartitionIndices) pairs representing
   *   how input partitions should be grouped together
   * - isGrouped: whether the output partitioning is grouped (no duplicates in partition keys)
   */
  @transient private lazy val groupedPartitionsTuple = {
    // There must be a `KeyedPartitioning` in child's output partitioning as a
    // `GroupPartitionsExec` node is added to a plan only in that case.
    val keyedPartitioning = child.outputPartitioning
      .asInstanceOf[Partitioning with Expression]
      .collectFirst { case k: KeyedPartitioning => k }
      .getOrElse(
        throw new SparkException("GroupPartitionsExec requires a child with KeyedPartitioning"))

    // Project partition keys if join key positions are specified
    val (projectedDataTypes, projectedKeys) =
      joinKeyPositions.fold(
        (keyedPartitioning.expressionDataTypes, keyedPartitioning.partitionKeys)
      )(keyedPartitioning.projectKeys)

    // Reduce keys if reducers are specified
    val (reducedDataTypes, reducedKeys) = reducers.fold((projectedDataTypes, projectedKeys))(
      KeyedPartitioning.reduceKeys(projectedKeys, projectedDataTypes, _))

    val keyToPartitionIndices = reducedKeys.zipWithIndex.groupMap(_._1)(_._2)

    if (expectedPartitionKeys.isDefined) {
      alignToExpectedKeys(keyToPartitionIndices)
    } else {
      (groupAndSortByKeys(keyToPartitionIndices, reducedDataTypes), true)
    }
  }

  @transient lazy val groupedPartitions: Seq[(InternalRowComparableWrapper, Seq[Int])] =
    groupedPartitionsTuple._1

  @transient lazy val isGrouped: Boolean = groupedPartitionsTuple._2

  // Whether the child subtree is safe to use with SortedMergeCoalescedRDD (k-way merge).
  //
  // --- The general problem ---
  //
  // Unlike a simple CoalescedRDD, which processes input partitions sequentially (open P0,
  // exhaust P0, open P1, exhaust P1, ...), SortedMergeCoalescedRDD must perform a k-way merge:
  // it opens *all* input partition iterators upfront and then interleaves reads across them to
  // produce a globally sorted output partition. Crucially, all of this happens on a single JVM
  // thread within a single Spark task.
  //
  // A SparkPlan object is shared across all partition computations of a task. When N partition
  // iterators are live concurrently on the same thread, N independent "computations" are all
  // operating through the same plan node instances. If any node in the subtree stores
  // per-partition state in an instance field rather than as a local variable captured inside the
  // partition's iterator closure, that field is aliased across all N concurrent computations.
  // Whichever computation last wrote the field "wins", and any computation that then reads or
  // frees it based on its own earlier write will operate on the wrong state.
  //
  // --- The correct pattern: PartitionEvaluatorFactory ---
  //
  // The PartitionEvaluatorFactory / PartitionEvaluator pattern is specifically designed to avoid
  // this problem. The factory's createEvaluator() is called once per partition and returns a
  // fresh PartitionEvaluator instance. All per-partition mutable state lives inside that
  // evaluator instance, not on the shared plan node. Operators that follow this pattern
  // exclusively (and hold no other mutable state on the plan node) are safe for k-way merge.
  //
  // --- Concrete example of an unsafe operator: SortExec + SortMergeJoinExec ---
  //
  // SortExec stores its active sorter in a plain var field (`rowSorter`) on the plan node.
  // When the k-way merge initialises its N partition iterators, each one drives the same SortExec
  // instance and calls createSorter(), which assigns rowSorter = newSorter -- overwriting the
  // field each time. After all N iterators are initialised, rowSorter holds only the sorter
  // created for the *last* partition.
  //
  // SortMergeJoinExec performs eager resource cleanup: when a join partition is exhausted it
  // calls cleanupResources() on its children, which reaches SortExec.cleanupResources(). That
  // method calls rowSorter.cleanupResources() -- but rowSorter now holds the last-created sorter,
  // not the one belonging to the just-exhausted partition. If that last sorter is still being
  // actively read by another partition in the k-way merge, freeing it causes a use-after-free.
  //
  // To be conservative, we use a whitelist: unknown node types fall through to unsafe, causing
  // a fallback to simple sequential coalescing. Only node types explicitly confirmed to store no
  // per-partition state in shared (plan node) instance fields are permitted.
  //
  // --- Interaction with late plan-tree mutations ---
  //
  // `outputOrdering` is first evaluated during EnsureRequirements (which decides whether to
  // add SortExec), but the child plan tree changes afterwards when
  // ApplyColumnarRulesAndInsertTransitions and CollapseCodegenStages insert wrapper nodes. The
  // correctness of this code relies on all such insertable nodes (WholeStageCodegenExec,
  // InputAdapter, ColumnarToRowExec) being in the SafeForKWayMerge whitelist so the evaluation
  // stays consistent.
  @transient private lazy val childIsSafeForKWayMerge: Boolean =
    !child.exists {
      case _: SafeForKWayMerge => false
      case _ => true
    }

  @transient private lazy val canUseSortedMerge: Boolean =
    SQLConf.get.v2BucketingPreserveOrderingOnCoalesceEnabled &&
      child.outputOrdering.nonEmpty &&
      childIsSafeForKWayMerge

  override protected def doExecute(): RDD[InternalRow] = {
    if (groupedPartitions.isEmpty) {
      sparkContext.emptyRDD
    } else if (canUseSortedMerge && groupedPartitions.exists(_._2.size > 1)) {
      val partitionCoalescer = new GroupedPartitionCoalescer(groupedPartitions.map(_._2))
      val rowOrdering = new LazyCodeGenOrdering(child.outputOrdering, child.output)
      new SortedMergeCoalescedRDD[InternalRow](
        child.execute(),
        groupedPartitions.size,
        partitionCoalescer,
        rowOrdering)
    } else {
      val partitionCoalescer = new GroupedPartitionCoalescer(groupedPartitions.map(_._2))
      new CoalescedRDD(child.execute(), groupedPartitions.size, Some(partitionCoalescer))
    }
  }

  override def supportsColumnar: Boolean =
    child.supportsColumnar && !(canUseSortedMerge && groupedPartitions.exists(_._2.size > 1))

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (groupedPartitions.isEmpty) {
      sparkContext.emptyRDD
    } else {
      val partitionCoalescer = new GroupedPartitionCoalescer(groupedPartitions.map(_._2))
      new CoalescedRDD(child.executeColumnar(), groupedPartitions.size, Some(partitionCoalescer))
    }
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def outputOrdering: Seq[SortOrder] = {
    if (groupedPartitions.forall(_._2.size <= 1)) {
      // No coalescing: each output partition is exactly one input partition. The child's
      // within-partition ordering is fully preserved (including any key-derived ordering that
      // `DataSourceV2ScanExecBase` already prepended).
      child.outputOrdering
    } else if (canUseSortedMerge) {
      // Coalescing with sorted merge: SortedMergeCoalescedRDD performs a k-way merge using the
      // child's ordering, so the full within-partition ordering is preserved end-to-end.
      child.outputOrdering
    } else {
      // Coalescing: multiple input partitions are merged into one output partition. The child's
      // within-partition ordering is lost due to concatenation -- for example, if two input
      // partitions both share key=A and hold rows (A,1),(A,3) and (A,2),(A,5) respectively (each
      // sorted ascending by the data column), concatenating them yields (A,1),(A,3),(A,2),(A,5)
      // which is no longer sorted by the data column. Only sort orders over partition key
      // expressions remain valid -- they evaluate to the same value (A) in every merged partition.
      outputPartitioning match {
        case p: Partitioning with Expression
            if reducers.isEmpty && conf.v2BucketingPreserveKeyOrderingOnCoalesceEnabled =>
          // Without reducers all merged partitions share the same original key value, so the key
          // expressions remain constant within the output partition. The child's outputOrdering
          // should already be in sync with the partitioning (either reported by the source or
          // derived from it in DataSourceV2ScanExecBase), so we only need to keep the sort orders
          // whose expression is a partition key expression -- all others are lost by concatenation.
          val keyedPartitionings = p.collect { case k: KeyedPartitioning => k }
          val keyExprs = ExpressionSet(keyedPartitionings.flatMap(_.expressions))
          child.outputOrdering.filter(order => keyExprs.contains(order.child))
        case _ =>
          // With reducers, merged partitions share only the reduced key, not the original key
          // expressions, which can take different values within the output partition.
          super.outputOrdering
      }
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"$nodeName${planSummaryParts(maxFields).map(" " + _).mkString("")}"
  }

  override protected def stringArgs: Iterator[Any] = planSummaryParts(Int.MaxValue)

  private def planSummaryParts(joinKeyMaxFields: Int): Iterator[String] = {
    val joinKeyStr = joinKeyPositions.map { p =>
      s"JoinKeyPositions: ${truncatedString(p, "[", ", ", "]", joinKeyMaxFields)}"
    }.iterator
    val expectedStr = expectedPartitionKeys.map(ks => s"ExpectedPartitionKeys: ${ks.size}")
    val reducersStr = reducers.map { seq =>
      val names = seq.map(_.map(_.displayName()).getOrElse("identity"))
      s"Reducers: ${truncatedString(names, "[", ", ", "]", joinKeyMaxFields)}"
    }
    val distributeStr = Iterator(s"DistributePartitions: $distributePartitions")
    joinKeyStr ++ expectedStr ++ reducersStr ++ distributeStr

  }
}

/**
 * A PartitionCoalescer that groups partitions according to a pre-computed grouping plan.
 *
 * Unlike Spark's default coalescer which tries to minimize data movement, this coalescer
 * groups partitions based on their partition keys to maintain the grouping semantics
 * required for storage-partitioned operations.
 *
 * @param groupedPartitions Sequence where each element is a sequence of input partition
 *                         indices that should be grouped together
 */
class GroupedPartitionCoalescer(
    val groupedPartitions: Seq[Seq[Int]]
  ) extends PartitionCoalescer with Serializable {

  override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {
    groupedPartitions.map { partitionIndices =>
      val partitions = new ArrayBuffer[Partition](partitionIndices.size)
      val preferredLocations = new ArrayBuffer[String](partitionIndices.size)
      partitionIndices.foreach { partitionIndex =>
        val partition = parent.partitions(partitionIndex)
        partitions += partition
        preferredLocations ++= parent.preferredLocations(partition)
      }
      // Select the most common location as the preferred location
      val preferredLocation = preferredLocations
        .groupBy(identity)
        .view.mapValues(_.size)
        .maxByOption(_._2)
        .map(_._1)
      val partitionGroup = new PartitionGroup(preferredLocation)
      partitionGroup.partitions ++= partitions
      partitionGroup
    }.toArray
  }
}

/**
 * A serializable [[Ordering]] for [[InternalRow]] that generates code-compiled comparison logic
 * lazily on first use. The [[SortOrder]] expressions and output schema are serialized with the
 * RDD; the generated comparator is rebuilt on the executor on first comparison via
 * [[GenerateOrdering]].
 */
private class LazyCodeGenOrdering(
    sortOrders: Seq[SortOrder],
    schema: Seq[Attribute]) extends Ordering[InternalRow] with Serializable {
  @transient private lazy val generated: Ordering[InternalRow] =
    GenerateOrdering.generate(sortOrders, schema)
  override def compare(x: InternalRow, y: InternalRow): Int = generated.compare(x, y)
}
