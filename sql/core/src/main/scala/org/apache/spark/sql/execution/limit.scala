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
import org.apache.spark.rdd.{ParallelCollectionRDD, RDD}
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, LazilyGeneratedOrdering}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.{SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.execution.python.HybridRowQueue
import org.apache.spark.util.collection.Utils

/**
 * The operator takes limited number of elements from its child operator.
 */
trait LimitExec extends UnaryExecNode {
  /** Number of element should be taken from child operator */
  def limit: Int
}

/**
 * Take the first `limit` elements, collect them to a single partition and then to drop the
 * first `offset` elements.
 *
 * This operator will be used when a logical `Limit` and/or `Offset` operation is the final operator
 * in an logical plan, which happens when the user is collecting results back to the driver.
 */
case class CollectLimitExec(limit: Int = -1, child: SparkPlan, offset: Int = 0) extends LimitExec {
  assert(limit >= 0 || (limit == -1 && offset > 0))

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = SinglePartition
  override def executeCollect(): Array[InternalRow] = {
    if (limit >= 0) {
      if (offset > 0) {
        child.executeTake(limit).drop(offset)
      } else {
        child.executeTake(limit)
      }
    } else {
      child.executeCollect().drop(offset)
    }
  }
  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size)
  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics = readMetrics ++ writeMetrics
  protected override def doExecute(): RDD[InternalRow] = {
    val childRDD = child.execute()
    if (childRDD.getNumPartitions == 0 || limit == 0) {
      new ParallelCollectionRDD(sparkContext, Seq.empty[InternalRow], 1, Map.empty)
    } else {
      val singlePartitionRDD = if (childRDD.getNumPartitions == 1) {
        childRDD
      } else {
        val locallyLimited = if (limit > 0) {
          childRDD.mapPartitionsInternal(_.take(limit))
        } else {
          childRDD
        }
        new ShuffledRowRDD(
          ShuffleExchangeExec.prepareShuffleDependency(
            locallyLimited,
            child.output,
            SinglePartition,
            serializer,
            writeMetrics),
          readMetrics)
      }
      if (limit >= 0) {
        if (offset > 0) {
          singlePartitionRDD.mapPartitionsInternal(_.slice(offset, limit))
        } else {
          singlePartitionRDD.mapPartitionsInternal(_.take(limit))
        }
      } else {
        singlePartitionRDD.mapPartitionsInternal(_.drop(offset))
      }
    }
  }

  override def stringArgs: Iterator[Any] = {
    super.stringArgs.zipWithIndex.filter {
      case (0, 2) => false
      case _ => true
    }.map(_._1)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

/**
 * Take the last `limit` elements and collect them to a single partition.
 *
 * This operator will be used when a logical `Tail` operation is the final operator in an
 * logical plan, which happens when the user is collecting results back to the driver.
 */
case class CollectTailExec(limit: Int, child: SparkPlan) extends LimitExec {
  assert(limit >= 0)

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = SinglePartition
  override def executeCollect(): Array[InternalRow] = child.executeTail(limit)
  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size)
  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics = readMetrics ++ writeMetrics
  protected override def doExecute(): RDD[InternalRow] = {
    val childRDD = child.execute()
    if (childRDD.getNumPartitions == 0 || limit == 0) {
      new ParallelCollectionRDD(sparkContext, Seq.empty[InternalRow], 1, Map.empty)
    } else {
      val singlePartitionRDD = if (childRDD.getNumPartitions == 1) {
        childRDD
      } else {
        val locallyLimited = childRDD.mapPartitionsInternal(takeRight)
        new ShuffledRowRDD(
          ShuffleExchangeExec.prepareShuffleDependency(
            locallyLimited,
            child.output,
            SinglePartition,
            serializer,
            writeMetrics),
          readMetrics)
      }
      singlePartitionRDD.mapPartitionsInternal(takeRight)
    }
  }

  private def takeRight(iter: Iterator[InternalRow]): Iterator[InternalRow] = {
    if (iter.isEmpty) {
      Iterator.empty[InternalRow]
    } else {
      val context = TaskContext.get()
      val queue = HybridRowQueue.apply(context.taskMemoryManager(), output.size)
      context.addTaskCompletionListener[Unit](_ => queue.close())
      var count = 0
      while (iter.hasNext) {
        queue.add(iter.next().copy().asInstanceOf[UnsafeRow])
        if (count < limit) {
          count += 1
        } else {
          queue.remove()
        }
      }
      Iterator.range(0, count).map(_ => queue.remove())
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

object BaseLimitExec {
  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  def newLimitCountTerm(): String = {
    val id = curId.getAndIncrement()
    s"_limit_counter_$id"
  }
}

/**
 * Helper trait which defines methods that are shared by both
 * [[LocalLimitExec]] and [[GlobalLimitExec]].
 */
trait BaseLimitExec extends LimitExec with CodegenSupport {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  protected override def doExecute(): RDD[InternalRow] = child.execute().mapPartitionsInternal {
    iter => iter.take(limit)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  // Mark this as empty. This plan doesn't need to evaluate any inputs and can defer the evaluation
  // to the parent operator.
  override def usedInputs: AttributeSet = AttributeSet.empty

  protected lazy val countTerm = BaseLimitExec.newLimitCountTerm()

  override lazy val limitNotReachedChecks: Seq[String] = if (limit >= 0) {
    s"$countTerm < $limit" +: super.limitNotReachedChecks
  } else {
    super.limitNotReachedChecks
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    // The counter name is already obtained by the upstream operators via `limitNotReachedChecks`.
    // Here we have to inline it to not change its name. This is fine as we won't have many limit
    // operators in one query.
    //
    // Note: create counter variable here instead of `doConsume()` to avoid compilation error,
    // because upstream operators might not call `doConsume()` here
    // (e.g. `HashJoin.codegenInner()`).
    ctx.addMutableState(CodeGenerator.JAVA_INT, countTerm, forceInline = true, useFreshName = false)
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    s"""
       | if ($countTerm < $limit) {
       |   $countTerm += 1;
       |   ${consume(ctx, input)}
       | }
     """.stripMargin
  }
}

/**
 * Take the first `limit` elements of each child partition, but do not collect or shuffle them.
 */
case class LocalLimitExec(limit: Int, child: SparkPlan) extends BaseLimitExec {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

/**
 * Take the first `limit` elements and then drop the first `offset` elements in the child's single
 * output partition.
 */
case class GlobalLimitExec(limit: Int = -1, child: SparkPlan, offset: Int = 0)
  extends BaseLimitExec {
  assert(limit >= 0 || (limit == -1 && offset > 0))

  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil

  override def doExecute(): RDD[InternalRow] = {
    if (offset > 0) {
      if (limit >= 0) {
        child.execute().mapPartitionsInternal(iter => iter.slice(offset, limit))
      } else {
        child.execute().mapPartitionsInternal(iter => iter.drop(offset))
      }
    } else {
      super.doExecute()
    }
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    if (offset > 0) {
      val skipTerm = ctx.addMutableState(CodeGenerator.JAVA_INT, "rowsSkipped", forceInline = true)
      if (limit > 0) {
        // In codegen, we skip the first `offset` rows, then take the first `limit - offset` rows.
        val finalLimit = limit - offset
        s"""
           | if ($skipTerm < $offset) {
           |   $skipTerm += 1;
           | } else if ($countTerm < $finalLimit) {
           |   $countTerm += 1;
           |   ${consume(ctx, input)}
           | }
         """.stripMargin
      } else {
        s"""
           | if ($skipTerm < $offset) {
           |   $skipTerm += 1;
           | } else {
           |   ${consume(ctx, input)}
           | }
         """.stripMargin
      }
    } else {
      super.doConsume(ctx, input, row)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

/**
 * Take the first `limit` elements as defined by the sortOrder, then drop the first `offset`
 * elements, and do projection if needed. This is logically equivalent to having a Limit and/or
 * Offset operator after a [[SortExec]] operator, or having a [[ProjectExec]] operator between them.
 * This could have been named TopK, but Spark's top operator does the opposite in ordering
 * so we name it TakeOrdered to avoid confusion.
 */
case class TakeOrderedAndProjectExec(
    limit: Int,
    sortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: SparkPlan,
    offset: Int = 0) extends OrderPreservingUnaryExecNode {

  override def output: Seq[Attribute] = {
    projectList.map(_.toAttribute)
  }

  override def executeCollect(): Array[InternalRow] = executeQuery {
    val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, sortOrder)
    val ord = new LazilyGeneratedOrdering(sortOrder, child.output)
    val limited = if (orderingSatisfies) {
      child.execute().mapPartitionsInternal(_.map(_.copy()).take(limit)).takeOrdered(limit)(ord)
    } else {
      child.execute().mapPartitionsInternal(_.map(_.copy())).takeOrdered(limit)(ord)
    }
    val data = if (offset > 0) limited.drop(offset) else limited
    if (projectList != child.output) {
      val proj = UnsafeProjection.create(projectList, child.output)
      proj.initialize(0)
      data.map(r => proj(r).copy())
    } else {
      data
    }
  }

  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size)

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics = readMetrics ++ writeMetrics

  protected override def doExecute(): RDD[InternalRow] = {
    val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, sortOrder)
    val ord = new LazilyGeneratedOrdering(sortOrder, child.output)
    val childRDD = child.execute()
    if (childRDD.getNumPartitions == 0) {
      new ParallelCollectionRDD(sparkContext, Seq.empty[InternalRow], 1, Map.empty)
    } else {
      val singlePartitionRDD = if (childRDD.getNumPartitions == 1) {
        childRDD
      } else {
        val localTopK = if (orderingSatisfies) {
          childRDD.mapPartitionsInternal(_.map(_.copy()).take(limit))
        } else {
          childRDD.mapPartitionsInternal { iter =>
            Utils.takeOrdered(iter.map(_.copy()), limit)(ord)
          }
        }

        new ShuffledRowRDD(
          ShuffleExchangeExec.prepareShuffleDependency(
            localTopK,
            child.output,
            SinglePartition,
            serializer,
            writeMetrics),
          readMetrics)
      }
      singlePartitionRDD.mapPartitionsWithIndexInternal { (idx, iter) =>
        val limited = Utils.takeOrdered(iter.map(_.copy()), limit)(ord)
        val topK = if (offset > 0) limited.drop(offset) else limited
        if (projectList != child.output) {
          val proj = UnsafeProjection.create(projectList, child.output)
          proj.initialize(idx)
          topK.map(r => proj(r))
        } else {
          topK
        }
      }
    }
  }

  override protected def outputExpressions: Seq[NamedExpression] = projectList

  override protected def orderingExpressions: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = SinglePartition

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(sortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    val offsetStr = if (offset > 0) s" offset=$offset," else ""
    s"TakeOrderedAndProject(limit=$limit,$offsetStr orderBy=$orderByString, output=$outputString)"
  }

  override def stringArgs: Iterator[Any] = {
    super.stringArgs.zipWithIndex.filter {
      case (0, 4) => false
      case _ => true
    }.map(_._1)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
