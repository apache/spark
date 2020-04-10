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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}


/**
 * A wrapper of shuffle query stage, which follows the given partition arrangement.
 *
 * @param child           It is usually `ShuffleQueryStageExec`, but can be the shuffle exchange
 *                        node during canonicalization.
 * @param partitionSpecs  The partition specs that defines the arrangement.
 */
case class CustomShuffleReaderExec private(
    child: SparkPlan,
    partitionSpecs: Seq[ShufflePartitionSpec]) extends UnaryExecNode {
  // If this reader is to read shuffle files locally, then all partition specs should be
  // `PartialMapperPartitionSpec`.
  if (partitionSpecs.exists(_.isInstanceOf[PartialMapperPartitionSpec])) {
    assert(partitionSpecs.forall(_.isInstanceOf[PartialMapperPartitionSpec]))
  }

  override def output: Seq[Attribute] = child.output
  override lazy val outputPartitioning: Partitioning = {
    // If it is a local shuffle reader with one mapper per task, then the output partitioning is
    // the same as the plan before shuffle.
    // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
    if (partitionSpecs.forall(_.isInstanceOf[PartialMapperPartitionSpec]) &&
        partitionSpecs.map(_.asInstanceOf[PartialMapperPartitionSpec].mapIndex).toSet.size ==
          partitionSpecs.length) {
      child match {
        case ShuffleQueryStageExec(_, s: ShuffleExchangeExec) =>
          s.child.outputPartitioning
        case ShuffleQueryStageExec(_, r @ ReusedExchangeExec(_, s: ShuffleExchangeExec)) =>
          s.child.outputPartitioning match {
            case e: Expression => r.updateAttr(e).asInstanceOf[Partitioning]
            case other => other
          }
        case _ =>
          throw new IllegalStateException("operating on canonicalization plan")
      }
    } else {
      UnknownPartitioning(partitionSpecs.length)
    }
  }

  override def stringArgs: Iterator[Any] = {
    val desc = if (isLocalReader) {
      "local"
    } else if (hasCoalescedPartition && hasSkewedPartition) {
      "coalesced and skewed"
    } else if (hasCoalescedPartition) {
      "coalesced"
    } else if (hasSkewedPartition) {
      "skewed"
    } else {
      ""
    }
    Iterator(desc)
  }

  def hasCoalescedPartition: Boolean =
    partitionSpecs.exists(_.isInstanceOf[CoalescedPartitionSpec])

  def hasSkewedPartition: Boolean =
    partitionSpecs.exists(_.isInstanceOf[PartialReducerPartitionSpec])

  def isLocalReader: Boolean =
    partitionSpecs.exists(_.isInstanceOf[PartialMapperPartitionSpec])

  private def shuffleStage = child match {
    case stage: ShuffleQueryStageExec => Some(stage)
    case _ => None
  }

  private def partitionDataSizeMetrics = {
    val maxSize = SQLMetrics.createSizeMetric(sparkContext, "maximum partition data size")
    val minSize = SQLMetrics.createSizeMetric(sparkContext, "minimum partition data size")
    val avgSize = SQLMetrics.createSizeMetric(sparkContext, "average partition data size")
    val mapStatsOpt = shuffleStage.get.mapStats
    val sizes = mapStatsOpt.map { mapStats =>
      val mapSizes = mapStats.bytesByPartitionId
      partitionSpecs.map {
        case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
          startReducerIndex.until(endReducerIndex).map(mapSizes).sum
        case p: PartialReducerPartitionSpec => p.dataSize
        case p => throw new IllegalStateException("unexpected " + p)
      }
    }.getOrElse(Seq(0L))

    maxSize.set(sizes.max)
    minSize.set(sizes.min)
    avgSize.set(sizes.sum / sizes.length)
    Map(
      "maxPartitionDataSize" -> maxSize,
      "minPartitionDataSize" -> minSize,
      "avgPartitionDataSize" -> avgSize)
  }

  private def skewedPartitionMetrics = {
    val metrics = SQLMetrics.createMetric(sparkContext, "number of skewed partitions")
    val numSkewedPartitions = partitionSpecs.collect {
      case p: PartialReducerPartitionSpec => p.reducerIndex
    }.distinct.length
    metrics.set(numSkewedPartitions)
    Map("numSkewedPartitions" -> metrics)
  }

  @transient override lazy val metrics: Map[String, SQLMetric] = {
    if (shuffleStage.isDefined) {
      val numPartitions = SQLMetrics.createMetric(sparkContext, "number of partitions")
      numPartitions.set(partitionSpecs.length)
      Map("numPartitions" -> numPartitions) ++ {
        if (isLocalReader) {
          // We split the mapper partition evenly when creating local shuffle reader, so no
          // data size info is available.
          Map.empty
        } else {
          partitionDataSizeMetrics
        }
      } ++ {
        if (hasSkewedPartition) {
          skewedPartitionMetrics
        } else {
          Map.empty
        }
      }
    } else {
      // It's a canonicalized plan, no need to report metrics.
      Map.empty
    }
  }

  private lazy val cachedShuffleRDD: RDD[InternalRow] = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
    shuffleStage.map { stage =>
      new ShuffledRowRDD(
        stage.shuffle.shuffleDependency, stage.shuffle.readMetrics, partitionSpecs.toArray)
    }.getOrElse {
      throw new IllegalStateException("operating on canonicalized plan")
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    cachedShuffleRDD
  }
}
