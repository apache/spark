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

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, LazilyGeneratedOrdering}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.util.Utils

/**
 * Take the first `limit` elements and collect them to a single partition.
 *
 * This operator will be used when a logical `Limit` operation is the final operator in an
 * logical plan, which happens when the user is collecting results back to the driver.
 */
case class CollectLimitExec(limit: Int, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = SinglePartition
  override def executeCollect(): Array[InternalRow] = child.executeTake(limit)
  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size)
  protected override def doExecute(): RDD[InternalRow] = {
    val locallyLimited = child.execute().mapPartitionsInternal(_.take(limit))
    val shuffled = new ShuffledRowRDD(
      ShuffleExchange.prepareShuffleDependency(
        locallyLimited, child.output, SinglePartition, serializer))
    shuffled.mapPartitionsInternal(_.take(limit))
  }
}

/**
 * Take the first `limit` elements of each child partition, but do not collect or shuffle them.
 */
case class LocalLimitExec(limit: Int, child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected override def doExecute(): RDD[InternalRow] = child.execute().mapPartitions { iter =>
    iter.take(limit)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val stopEarly = ctx.freshName("stopEarly")
    ctx.addMutableState("boolean", stopEarly, s"$stopEarly = false;")

    ctx.addNewFunction("stopEarly", s"""
      @Override
      protected boolean stopEarly() {
        return $stopEarly;
      }
    """, inlineToOuterClass = true)
    val countTerm = ctx.freshName("count")
    ctx.addMutableState("int", countTerm, s"$countTerm = 0;")
    s"""
       | if ($countTerm < $limit) {
       |   $countTerm += 1;
       |   ${consume(ctx, input)}
       | } else {
       |   $stopEarly = true;
       | }
     """.stripMargin
  }
}

/**
 * Take the `limit` elements of the child output.
 */
case class GlobalLimitExec(limit: Int, child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size)

  protected override def doExecute(): RDD[InternalRow] = {
    val childRDD = child.execute()
    val partitioner = LocalPartitioning(child.outputPartitioning,
      childRDD.getNumPartitions)
    val shuffleDependency = ShuffleExchange.prepareShuffleDependency(
      childRDD, child.output, partitioner, serializer)
    val numberOfOutput: Seq[Long] = if (shuffleDependency.rdd.getNumPartitions != 0) {
      // submitMapStage does not accept RDD with 0 partition.
      // So, we will not submit this dependency.
      val submittedStageFuture = sparkContext.submitMapStage(shuffleDependency)
      submittedStageFuture.get().recordsByPartitionId.toSeq
    } else {
      Nil
    }

    // Try to keep child plan's original data parallelism or not. It is enabled by default.
    val respectChildParallelism = sqlContext.conf.enableParallelGlobalLimit

    val shuffled = new ShuffledRowRDD(shuffleDependency)

    val sumOfOutput = numberOfOutput.sum
    if (sumOfOutput <= limit) {
      shuffled
    } else if (!respectChildParallelism) {
      // This is mainly for tests.
      // Some tests like hive compatibility tests assume that the rows are returned by a specified
      // order that the partitions are scaned sequentially until we reach the required number of
      // rows. However, logically a Limit operator should not care the row scan order.
      // Thus we take the rows of each partition until we reach the required limit number.
      var numTakenRow = 0
      val takeAmounts = new mutable.HashMap[Int, Int]()
      numberOfOutput.zipWithIndex.foreach { case (num, index) =>
        if (numTakenRow + num < limit) {
          numTakenRow += num.toInt
          takeAmounts += ((index, num.toInt))
        } else {
          val toTake = limit - numTakenRow
          numTakenRow += toTake
          takeAmounts += ((index, toTake))
        }
      }
      val broadMap = sparkContext.broadcast(takeAmounts)
      shuffled.mapPartitionsWithIndexInternal { case (index, iter) =>
        broadMap.value.get(index).map { size =>
          iter.take(size)
        }.get
      }
    } else {
      // We try to evenly require the asked limit number of rows across all child rdd's partitions.
      var rowsNeedToTake: Long = limit
      val takeAmountByPartition: Array[Long] = Array.fill[Long](numberOfOutput.length)(0L)
      val remainingRowsByPartition: Array[Long] = Array(numberOfOutput: _*)

      while (rowsNeedToTake > 0) {
        val nonEmptyParts = remainingRowsByPartition.count(_ > 0)
        // If the rows needed to take are less the number of non-empty partitions, take one row from
        // each non-empty partitions until we reach `limit` rows.
        // Otherwise, evenly divide the needed rows to each non-empty partitions.
        val takePerPart = math.max(1, rowsNeedToTake / nonEmptyParts)
        remainingRowsByPartition.zipWithIndex.foreach { case (num, index) =>
          // In case `rowsNeedToTake` < `nonEmptyParts`, we may run out of `rowsNeedToTake` during
          // the traversal, so we need to add this check.
          if (rowsNeedToTake > 0 && num > 0) {
            if (num >= takePerPart) {
              rowsNeedToTake -= takePerPart
              takeAmountByPartition(index) += takePerPart
              remainingRowsByPartition(index) -= takePerPart
            } else {
              rowsNeedToTake -= num
              takeAmountByPartition(index) += num
              remainingRowsByPartition(index) -= num
            }
          }
        }
      }
      val broadMap = sparkContext.broadcast(takeAmountByPartition)
      shuffled.mapPartitionsWithIndexInternal { case (index, iter) =>
        iter.take(broadMap.value(index).toInt)
      }
    }
  }
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

  protected override def doExecute(): RDD[InternalRow] = {
    val ord = new LazilyGeneratedOrdering(sortOrder, child.output)
    val localTopK: RDD[InternalRow] = {
      child.execute().map(_.copy()).mapPartitions { iter =>
        org.apache.spark.util.collection.Utils.takeOrdered(iter, limit)(ord)
      }
    }
    val shuffled = new ShuffledRowRDD(
      ShuffleExchange.prepareShuffleDependency(
        localTopK, child.output, SinglePartition, serializer))
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

  override def simpleString: String = {
    val orderByString = Utils.truncatedString(sortOrder, "[", ",", "]")
    val outputString = Utils.truncatedString(output, "[", ",", "]")

    s"TakeOrderedAndProject(limit=$limit, orderBy=$orderByString, output=$outputString)"
  }
}
