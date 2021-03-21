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

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, LazilyGeneratedOrdering}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.{SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}

/**
 * The operator takes limited number of elements from its child operator.
 */
trait LimitExec extends UnaryExecNode {
  /** Number of element should be taken from child operator */
  def limit: Int
}

/**
 * Take the first `limit` elements and collect them to a single partition.
 *
 * This operator will be used when a logical `Limit` operation is the final operator in an
 * logical plan, which happens when the user is collecting results back to the driver.
 */
case class CollectLimitExec(limit: Int, child: SparkPlan) extends LimitExec {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = SinglePartition
  override def executeCollect(): Array[InternalRow] = child.executeTake(limit)
  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size)
  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics = readMetrics ++ writeMetrics
  protected override def doExecute(): RDD[InternalRow] = {
    val locallyLimited = child.execute().mapPartitionsInternal(_.take(limit))
    val shuffled = new ShuffledRowRDD(
      ShuffleExchangeExec.prepareShuffleDependency(
        locallyLimited,
        child.output,
        SinglePartition,
        serializer,
        writeMetrics),
      readMetrics)
    shuffled.mapPartitionsInternal(_.take(limit))
  }
}

/**
 * Take the last `limit` elements and collect them to a single partition.
 *
 * This operator will be used when a logical `Tail` operation is the final operator in an
 * logical plan, which happens when the user is collecting results back to the driver.
 */
case class CollectTailExec(limit: Int, child: SparkPlan) extends LimitExec {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = SinglePartition
  override def executeCollect(): Array[InternalRow] = child.executeTail(limit)
  protected override def doExecute(): RDD[InternalRow] = {
    // This is a bit hacky way to avoid a shuffle and scanning all data when it performs
    // at `Dataset.tail`.
    // Since this execution plan and `execute` are currently called only when
    // `Dataset.tail` is invoked, the jobs are always executed when they are supposed to be.

    // If we use this execution plan separately like `Dataset.limit` without an actual
    // job launch, we might just have to mimic the implementation of `CollectLimitExec`.
    sparkContext.parallelize(executeCollect(), numSlices = 1)
  }
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

  protected override def doExecute(): RDD[InternalRow] = child.execute().mapPartitions { iter =>
    iter.take(limit)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  // Mark this as empty. This plan doesn't need to evaluate any inputs and can defer the evaluation
  // to the parent operator.
  override def usedInputs: AttributeSet = AttributeSet.empty

  private lazy val countTerm = BaseLimitExec.newLimitCountTerm()

  override lazy val limitNotReachedChecks: Seq[String] = {
    s"$countTerm < $limit" +: super.limitNotReachedChecks
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

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

/**
 * Take the first `limit` elements of the child's single output partition.
 */
case class GlobalLimitExec(limit: Int, child: SparkPlan) extends BaseLimitExec {

  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

/**
 * Take the first limit elements as defined by the sortOrder, and do projection if needed.
 * This is logically equivalent to having a Limit operator after a [[SortExec]] operator,
 * or having a [[ProjectExec]] operator between them.
 * This could have been named TopK, but Spark's top operator does the opposite in ordering
 * so we name it TakeOrdered to avoid confusion.
 */
case class TakeOrderedAndProjectExec(
    limit: Int,
    sortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = {
    projectList.map(_.toAttribute)
  }

  override def executeCollect(): Array[InternalRow] = {
    val ord = new LazilyGeneratedOrdering(sortOrder, child.output)
    val data = child.execute().map(_.copy()).takeOrdered(limit)(ord)
    if (projectList != child.output) {
      val proj = UnsafeProjection.create(projectList, child.output)
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
    val ord = new LazilyGeneratedOrdering(sortOrder, child.output)
    val localTopK: RDD[InternalRow] = {
      child.execute().map(_.copy()).mapPartitions { iter =>
        org.apache.spark.util.collection.Utils.takeOrdered(iter, limit)(ord)
      }
    }
    val shuffled = new ShuffledRowRDD(
      ShuffleExchangeExec.prepareShuffleDependency(
        localTopK,
        child.output,
        SinglePartition,
        serializer,
        writeMetrics),
      readMetrics)
    shuffled.mapPartitions { iter =>
      val topK = org.apache.spark.util.collection.Utils.takeOrdered(iter.map(_.copy()), limit)(ord)
      if (projectList != child.output) {
        val proj = UnsafeProjection.create(projectList, child.output)
        topK.map(r => proj(r))
      } else {
        topK
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = SinglePartition

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(sortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    s"TakeOrderedAndProject(limit=$limit, orderBy=$orderByString, output=$outputString)"
  }
}
