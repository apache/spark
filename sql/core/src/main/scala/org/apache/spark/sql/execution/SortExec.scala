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

import java.util.concurrent.TimeUnit._

import scala.collection.mutable

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * Performs (external) sorting.
 *
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 * @param testSpillFrequency Method for configuring periodic spilling in unit tests. If set, will
 *                           spill every `frequency` records.
 */
case class SortExec(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    testSpillFrequency: Int = 0)
  extends UnaryExecNode with BlockingOperatorWithCodegen {

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  // sort performed is local within a given partition so will retain
  // child operator's partitioning
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  private val enableRadixSort = conf.enableRadixSort

  override lazy val metrics = Map(
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "sort time"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  /**
   * Creates the sorter of this node and keeps it for [[cleanupResources]]. The generated code
   * references this holder instead of the node, so that the node and its child subtree are not
   * serialized into every task. The node keeps the same holder through serialization, so the
   * sorter the generated code creates is the one that [[cleanupResources]] of this node (e.g.
   * reached through the interpreted `SortMergeJoinExec`) closes, as long as both are
   * deserialized from the same task binary.
   */
  private[sql] lazy val sorterHolder =
    new SortExecSorterHolder(sortOrder, output, enableRadixSort, testSpillFrequency)

  /**
   * This method gets invoked only once for each SortExec instance to initialize an
   * UnsafeExternalRowSorter. The generated code calls [[SortExecSorterHolder.createSorter]]
   * directly.
   */
  def createSorter(): UnsafeExternalRowSorter = sorterHolder.createSorter()

  protected override def doExecute(): RDD[InternalRow] = {
    val peakMemory = longMetric("peakMemory")
    val spillSize = longMetric("spillSize")
    val sortTime = longMetric("sortTime")

    child.execute().mapPartitionsInternal { iter =>
      val sorter = createSorter()

      val metrics = TaskContext.get().taskMetrics()
      // Remember spill data size of this task before execute this operator so that we can
      // figure out how many bytes we spilled for this operator.
      val spillSizeBefore = metrics.memoryBytesSpilled
      val sortedIterator = sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
      SortExec.recordSortMetrics(sorter, metrics, spillSizeBefore, sortTime, peakMemory, spillSize)

      sortedIterator
    }
  }

  override def usedInputs: AttributeSet = AttributeSet(Seq.empty)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  // Name of sorter variable used in codegen.
  private var sorterVariable: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    val needToSort =
      ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "needToSort", v => s"$v = true;")

    // Initialize the class member variables. This includes the instance of the Sorter and
    // the iterator to return sorted rows.
    val holder = ctx.addReferenceObj("sorterHolder", sorterHolder)
    // Inline mutable state since not many Sort operations in a task
    sorterVariable = ctx.addMutableState(classOf[UnsafeExternalRowSorter].getName, "sorter",
      v => s"$v = $holder.createSorter();", forceInline = true)
    val metrics = ctx.addMutableState(classOf[TaskMetrics].getName, "metrics",
      v => s"$v = org.apache.spark.TaskContext.get().taskMetrics();", forceInline = true)
    val sortedIterator = ctx.addMutableState("scala.collection.Iterator<InternalRow>", "sortedIter",
      forceInline = true)

    val addToSorter = ctx.freshName("addToSorter")
    // Pass `partitionIndex` as a parameter so bare references in the child's
    // produce resolve to the local, not the protected superclass field.
    // Required when `addNewFunction` spills this helper to a nested class.
    val addToSorterFuncName = ctx.addNewFunction(addToSorter,
      s"""
        | private void $addToSorter(int partitionIndex) throws java.io.IOException {
        |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
        | }
      """.stripMargin.trim)

    val outputRow = ctx.freshName("outputRow")
    val peakMemory = metricTerm(ctx, "peakMemory")
    val spillSize = metricTerm(ctx, "spillSize")
    val spillSizeBefore = ctx.freshName("spillSizeBefore")
    val sortTime = metricTerm(ctx, "sortTime")
    s"""
       | if ($needToSort) {
       |   long $spillSizeBefore = $metrics.memoryBytesSpilled();
       |   $addToSorterFuncName(partitionIndex);
       |   $sortedIterator = $sorterVariable.sort();
       |   org.apache.spark.sql.execution.SortExec.recordSortMetrics(
       |     $sorterVariable, $metrics, $spillSizeBefore, $sortTime, $peakMemory, $spillSize);
       |   $needToSort = false;
       | }
       |
       | while ($limitNotReachedCond $sortedIterator.hasNext()) {
       |   UnsafeRow $outputRow = (UnsafeRow)$sortedIterator.next();
       |   ${consume(ctx, null, outputRow)}
       |   if (shouldStop()) return;
       | }
     """.stripMargin.trim
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    s"""
       |${row.code}
       |$sorterVariable.insertRow((UnsafeRow)${row.value});
     """.stripMargin
  }

  /**
   * In SortExec, we overwrite cleanupResources to close UnsafeExternalRowSorter.
   * There's possible for the sorter to be null here, for example, in the scenario of empty
   * iterator in the current task, the downstream physical node (like SortMergeJoinExec) will
   * trigger cleanupResources before the sorter is initialized in createSorter.
   */
  override protected[sql] def cleanupResources(): Unit = {
    sorterHolder.cleanupResources()
    super.cleanupResources()
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SortExec =
    copy(child = newChild)
}

object SortExec {
  /**
   * Records the sort metrics (sort time, peak memory and spill size) for a completed sort. This
   * is called both by the interpreted path ([[SortExec.doExecute]]) and by the generated code of
   * [[SortExec]] (through the static forwarder), so the type-independent metric bookkeeping is
   * compiled once per JVM instead of being re-emitted into every SortExec stage's generated code.
   *
   * `spillSizeBefore` is the task's spilled-bytes count captured before the rows were fed to the
   * sorter, so that `spillSize` reflects only this operator's contribution.
   */
  def recordSortMetrics(
      sorter: UnsafeExternalRowSorter,
      metrics: TaskMetrics,
      spillSizeBefore: Long,
      sortTime: SQLMetric,
      peakMemory: SQLMetric,
      spillSize: SQLMetric): Unit = {
    sortTime += NANOSECONDS.toMillis(sorter.getSortTimeNanos)
    peakMemory += sorter.getPeakMemoryUsage
    spillSize += metrics.memoryBytesSpilled - spillSizeBefore
    metrics.incPeakExecutionMemory(sorter.getPeakMemoryUsage)
  }

  /**
   * The sorter holders of the [[SortExec]]s that run in the same task as `plan`: those in its
   * subtree, but not beyond an [[Exchange]]. A join that releases its children's resources early
   * from its generated code (see [[genCleanupSorters]]) references these instead of its child
   * plans, so that the plans are not serialized into every task.
   */
  private[sql] def sorterHoldersInStage(plan: SparkPlan): Array[SortExecSorterHolder] = {
    val holders = mutable.ArrayBuffer[SortExecSorterHolder]()
    def visit(p: SparkPlan): Unit = p match {
      case _: Exchange =>
      case s: SortExec =>
        holders += s.sorterHolder
        visit(s.child)
      case other => other.children.foreach(visit)
    }
    visit(plan)
    holders.toArray
  }

  /**
   * Generates the code that closes the sorters of the [[SortExec]]s below `join`, for a join that
   * releases its children's resources early. This does in generated code what
   * `join.cleanupResources()` does, without referencing the plan: only the sorter holders from
   * [[sorterHoldersInStage]]. The other plan nodes that override [[SparkPlan.cleanupResources]]
   * hold nothing to release in a task.
   */
  private[sql] def genCleanupSorters(ctx: CodegenContext, join: SparkPlan): String = {
    val holders = ctx.addReferenceObj("sorterHolders", sorterHoldersInStage(join),
      s"${classOf[SortExecSorterHolder].getName}[]")
    s"${classOf[SortExec].getName}.cleanupSorters($holders);"
  }

  /** Closes the sorters of `holders`. Called from the code of [[genCleanupSorters]]. */
  def cleanupSorters(holders: Array[SortExecSorterHolder]): Unit = {
    holders.foreach(_.cleanupResources())
  }
}

/**
 * Creates the sorter of a [[SortExec]] and holds it until [[cleanupResources]]. It holds only what
 * the sorter needs, not the plan, and is referenced by the generated code of the [[SortExec]] and
 * of joins that release the sorter early (see [[SortExec.sorterHoldersInStage]]).
 *
 * WARNING: the sorter is per-task mutable state. Each task deserializes its own copy of the
 * holder from the task binary, so it must not be shared across tasks or accessed from multiple
 * threads concurrently.
 */
private[sql] class SortExecSorterHolder(
    sortOrder: Seq[SortOrder],
    output: Seq[Attribute],
    enableRadixSort: Boolean,
    testSpillFrequency: Int) extends Serializable {

  @transient private var rowSorter: UnsafeExternalRowSorter = _

  def createSorter(): UnsafeExternalRowSorter = {
    val ordering = RowOrdering.create(sortOrder, output)

    // The comparator for comparing prefix
    val boundSortExpression = BindReferences.bindReference(sortOrder.head, output)
    val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

    val canUseRadixSort = enableRadixSort && sortOrder.length == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

    // The generator for prefix
    val prefixExpr = SortPrefix(boundSortExpression)
    val prefixProjection = UnsafeProjection.create(Seq(prefixExpr))
    val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
      private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
      override def computePrefix(row: InternalRow):
          UnsafeExternalRowSorter.PrefixComputer.Prefix = {
        val prefix = prefixProjection.apply(row)
        result.isNull = prefix.isNullAt(0)
        result.value = if (result.isNull) prefixExpr.nullValue else prefix.getLong(0)
        result
      }
    }

    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    rowSorter = UnsafeExternalRowSorter.create(DataTypeUtils.fromAttributes(output), ordering,
      prefixComparator, prefixComputer, pageSize, canUseRadixSort)

    if (testSpillFrequency > 0) {
      rowSorter.setTestSpillFrequency(testSpillFrequency)
    }
    rowSorter
  }

  /**
   * Closes the sorter, if any. It can be null, e.g. when a join above releases the resources of
   * its children before this sorter is created.
   */
  def cleanupResources(): Unit = {
    if (rowSorter != null) {
      rowSorter.cleanupResources()
    }
  }
}
