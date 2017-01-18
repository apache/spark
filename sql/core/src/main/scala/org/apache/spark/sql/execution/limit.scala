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

import scala.collection.mutable.HashMap

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

  protected override def doExecute(): RDD[InternalRow] = child.execute().mapPartitions { iter =>
    iter.take(limit)
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

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
    """)
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
 * Take the first `limit` elements of the child's partitions.
 */
case class GlobalLimitExec(limit: Int, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  protected override def doExecute(): RDD[InternalRow] = {
    // This logic is mainly copyed from `SparkPlan.executeTake`.
    // TODO: combine this with `SparkPlan.executeTake`, if possible.
    val childRDD = child.execute()
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    var totalNum = 0
    var resultRDD: RDD[InternalRow] = null
    while (totalNum < limit && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the previous iteration, quadruple and retry.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate
        // it by 50%. We also cap the estimation in the end.
        val limitScaleUpFactor = Math.max(sqlContext.conf.limitScaleUpFactor, 2)
        if (totalNum == 0) {
          numPartsToTry = partsScanned * limitScaleUpFactor
        } else {
          // the left side of max is >=1 whenever partsScanned >= 2
          numPartsToTry = Math.max((1.5 * limit * partsScanned / totalNum).toInt - partsScanned, 1)
          numPartsToTry = Math.min(numPartsToTry, partsScanned * limitScaleUpFactor)
        }
      }

      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val sc = sqlContext.sparkContext
      val res = sc.runJob(childRDD,
        (it: Iterator[InternalRow]) => Array[Int](it.size), p)

      totalNum += res.map(_.head).sum
      partsScanned += p.size

      if (totalNum >= limit) {
        // If we scan more rows than the limit number, we need to reduce that from scanned.
        // We calculate how many rows need to be reduced for each partition,
        // until all redunant rows are reduced.
        var numToReduce = (totalNum - limit)
        val reduceAmounts = new HashMap[Int, Int]()
        val partitionsToReduce = p.zip(res.map(_.head)).foreach { case (part, size) =>
          val toReduce = if (size > numToReduce) numToReduce else size
          reduceAmounts += ((part, toReduce))
          numToReduce -= toReduce
        }
        resultRDD = childRDD.mapPartitionsWithIndexInternal { case (index, iter) =>
          if (index < partsScanned) {
            // Those partitions are scanned.
            reduceAmounts.get(index).map { size =>
              iter.drop(size)
            }.getOrElse(iter)
          } else {
            // Those partitions are not scanned.
            Array.empty[InternalRow].toIterator
          }
        }
      }
    }
    // If totalNum is less than limit after we scan all partitions, just return all the data.
    if (resultRDD == null) {
      childRDD
    } else {
      resultRDD
    }
  }

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
