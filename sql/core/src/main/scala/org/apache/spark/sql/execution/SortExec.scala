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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MILLIS
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator

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
  extends SortExecBase(
    sortOrder,
    global,
    child,
    testSpillFrequency) {

  def createSorter(): UnsafeExternalRowSorter = {
    rowSorter = UnsafeExternalRowSorter.create(
      schema, ordering, prefixComparator, prefixComputer, pageSize, canUseRadixSort)

    if (testSpillFrequency > 0) {
      rowSorter.setTestSpillFrequency(testSpillFrequency)
    }
    rowSorter.asInstanceOf[UnsafeExternalRowSorter]
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    doProduce(ctx, classOf[UnsafeExternalRowSorter].getName)
  }
}

/**
 * Performs (external) sorting for multiple windows.
 *
 * @param partitionSpec a sequence of expressions that defines a partition key
 * @param sortOrderInWindow a sequence of sort orders for sorting rows inside a window
 * @param sortOrderAcrossWindows a sequence of sort orders for sorting rows across
 *                               different windows on a Spark physical partition.
 *                               This sequence of sort orders is obtained from a partition
 *                               key plus a sequence of sort orders inside a window
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 * @param testSpillFrequency Method for configuring periodic spilling in unit tests. If set, will
 *                           spill every `frequency` records.
 */
case class WindowSortExec(
    partitionSpec: Seq[Expression],
    sortOrderInWindow: Seq[SortOrder],
    sortOrderAcrossWindows: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    testSpillFrequency: Int = 0)
  extends SortExecBase(
    sortOrderAcrossWindows,
    global,
    child,
    testSpillFrequency) {

  def createSorter(): UnsafeExternalRowWindowSorter = {
    val partitionSpecGrouping = UnsafeProjection.create(partitionSpec, output)

    // The schema of partition key
    val partitionKeySchema: Seq[Attribute] = output.filter(x => {
      x.references.subsetOf(AttributeSet(partitionSpec))
    })

    // Generate the ordering of partition key
    val orderingOfPartitionKey = RowOrdering.create(
      sortOrderAcrossWindows diff sortOrderInWindow,
      partitionKeySchema)

    // No prefix comparator
    val nullPrefixComparator = new PrefixComparator {
      override def compare(prefix1: Long, prefix2: Long): Int = 0
    }

    if (sortOrderInWindow == null || sortOrderInWindow.size == 0) {
      rowSorter = UnsafeExternalRowWindowSorter.create(
        schema,
        partitionSpecGrouping,
        orderingOfPartitionKey,
        null,
        ordering,
        nullPrefixComparator,
        prefixComparator,
        createPrefixComputer(null),
        prefixComputer,
        false,
        canUseRadixSort,
        pageSize)
    } else {
      // Generate the bound expression in a window
      val boundSortExpressionInWindow = BindReferences.bindReference(
        sortOrderInWindow.head, output)

      // Generate the ordering based on sort order in a window
      val orderingInWindow = RowOrdering.create(sortOrderInWindow, output)

      // The expression for sort prefix in a window
      val sortPrefixExprInWindow = SortPrefix(boundSortExpressionInWindow)

      // The comparator for comparing prefix in a window
      val prefixComparatorInWindow = SortPrefixUtils.getPrefixComparator(
        boundSortExpressionInWindow)

      // The computer for prefix in a window
      val prefixComputerInWindow = createPrefixComputer(sortPrefixExprInWindow)

      // Can use radix sort or not in a window
      val canUseRadixSortInWindow = enableRadixSort && sortOrderInWindow.length == 1 &&
        SortPrefixUtils.canSortFullyWithPrefix(boundSortExpressionInWindow)

      rowSorter = UnsafeExternalRowWindowSorter.create(
        schema,
        partitionSpecGrouping,
        orderingOfPartitionKey,
        orderingInWindow,
        ordering,
        prefixComparatorInWindow,
        prefixComparator,
        prefixComputerInWindow,
        prefixComputer,
        canUseRadixSortInWindow,
        canUseRadixSort,
        pageSize)
    }

    if (testSpillFrequency > 0) {
      rowSorter.setTestSpillFrequency(testSpillFrequency)
    }
    rowSorter.asInstanceOf[UnsafeExternalRowWindowSorter]
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    doProduce(ctx, classOf[UnsafeExternalRowWindowSorter].getName)
  }
}

abstract class SortExecBase(
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

  val enableRadixSort = sqlContext.conf.enableRadixSort

  lazy val boundSortExpression = BindReferences.bindReference(sortOrder.head, output)
  lazy val ordering = RowOrdering.create(sortOrder, output)
  lazy val sortPrefixExpr = SortPrefix(boundSortExpression)

  // The comparator for comparing prefix
  lazy val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

  // The generator for prefix
  lazy val prefixComputer = createPrefixComputer(sortPrefixExpr)

  lazy val canUseRadixSort = enableRadixSort && sortOrder.length == 1 &&
    SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

  lazy val pageSize = SparkEnv.get.memoryManager.pageSizeBytes

  override lazy val metrics = Map(
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "sort time"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  private[sql] var rowSorter: AbstractUnsafeExternalRowSorter = _

  /**
   * This method gets invoked only once for each SortExecBase instance to initialize an
   * AbstractUnsafeExternalRowSorter, both `plan.execute` and code generation are using it.
   * In the code generation code path, we need to call this function outside the class so we
   * should make it public.
   */
  def createSorter(): AbstractUnsafeExternalRowSorter

  protected def createPrefixComputer(prefixExpr: SortPrefix):
      UnsafeExternalRowSorter.PrefixComputer = {
    if (prefixExpr != null) {
      val prefixProjection = UnsafeProjection.create(Seq(prefixExpr))

      new UnsafeExternalRowSorter.PrefixComputer {
        override def computePrefix(row: InternalRow):
            UnsafeExternalRowSorter.PrefixComputer.Prefix = {
          val prefix = prefixProjection.apply(row)
          new UnsafeExternalRowSorter.PrefixComputer.Prefix {
            isNull = prefix.isNullAt(0)
            value = if (prefix.isNullAt(0)) prefixExpr.nullValue else prefix.getLong(0)
          }
        }
      }
    } else {
      new UnsafeExternalRowSorter.PrefixComputer {
        override def computePrefix(row: InternalRow):
            UnsafeExternalRowSorter.PrefixComputer.Prefix = {
          new UnsafeExternalRowSorter.PrefixComputer.Prefix {
            isNull = false
            value = 0
          }
        }
      }
    }
  }

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
      sortTime += NANOSECONDS.toMillis(sorter.getSortTimeNanos)
      peakMemory += sorter.getPeakMemoryUsage
      spillSize += metrics.memoryBytesSpilled - spillSizeBefore
      metrics.incPeakExecutionMemory(sorter.getPeakMemoryUsage)

      sortedIterator
    }
  }

  override def usedInputs: AttributeSet = AttributeSet(Seq.empty)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  // Name of sorter variable used in codegen.
  private var sorterVariable: String = _

  protected def doProduce(ctx: CodegenContext, sortClassType: String): String = {
    val needToSort =
      ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "needToSort", v => s"$v = true;")

    // Initialize the class member variables. This includes the instance of the Sorter and
    // the iterator to return sorted rows.
    val thisPlan = ctx.addReferenceObj("plan", this)
    // Inline mutable state since not many Sort operations in a task
    sorterVariable = ctx.addMutableState(sortClassType, "sorter",
      v => s"$v = $thisPlan.createSorter();", forceInline = true)
    val metrics = ctx.addMutableState(classOf[TaskMetrics].getName, "metrics",
      v => s"$v = org.apache.spark.TaskContext.get().taskMetrics();", forceInline = true)
    val sortedIterator = ctx.addMutableState("scala.collection.Iterator<UnsafeRow>", "sortedIter",
      forceInline = true)

    val addToSorter = ctx.freshName("addToSorter")
    val addToSorterFuncName = ctx.addNewFunction(addToSorter,
      s"""
        | private void $addToSorter() throws java.io.IOException {
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
       |   $addToSorterFuncName();
       |   $sortedIterator = $sorterVariable.sort();
       |   $sortTime.add($sorterVariable.getSortTimeNanos() / $NANOS_PER_MILLIS);
       |   $peakMemory.add($sorterVariable.getPeakMemoryUsage());
       |   $spillSize.add($metrics.memoryBytesSpilled() - $spillSizeBefore);
       |   $metrics.incPeakExecutionMemory($sorterVariable.getPeakMemoryUsage());
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
   * In WindowSortExec, we overwrite cleanupResources to close UnsafeExternalRowWindowSorter.
   */
  override protected[sql] def cleanupResources(): Unit = {
    if (rowSorter != null) {
      // There's possible for rowSorter is null here, for example, in the scenario of empty
      // iterator in the current task, the downstream physical node(like SortMergeJoinExec) will
      // trigger cleanupResources before rowSorter initialized in createSorter.
      rowSorter.cleanupResources()
    }
    super.cleanupResources()
  }
}
