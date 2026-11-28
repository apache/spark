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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import org.apache.spark.{Dependency, NarrowDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, StorageFilterRequest}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.connector.{StorageFilterImpl, StorageFilterRequestImpl}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.util.ArrayImplicits._

/** A split of a [[PerPartitionStorageFilterRDD]], with the partitions of its bloom filters. */
private class PerPartitionStorageFilterPartition(
    index: Int,
    inputPartition: Option[InputPartition],
    val bloomPartitions: Array[Partition])
  extends DataSourceRDDPartition(index, inputPartition)

/**
 * The [[DataSourceRDD]] of a [[BatchScanExec]] with a per-partition bloom filter. The readers of
 * each split get `condition` with the bloom filters of the split's partitions of `blooms` in place
 * of `bloomFilter`, ORed into one. Splits of one key have the same partitions of `blooms`, so a
 * task that reads those splits one after another reads them once.
 *
 * @param bloomPartitions for each split, the partitions of `blooms` that report its key
 * @param bloomOrdinal the position of the bloom filter column in the rows of `blooms`
 */
private class PerPartitionStorageFilterRDD(
    sc: SparkContext,
    inputPartitions: Seq[Option[InputPartition]],
    partitionReaderFactory: PartitionReaderFactory,
    columnarReads: Boolean,
    customMetrics: Map[String, SQLMetric],
    checkedOutputWidth: Option[Int],
    @transient private val blooms: RDD[InternalRow],
    @transient private val bloomPartitions: Array[Array[Int]],
    bloomOrdinal: Int,
    condition: Expression,
    bloomFilter: Attribute,
    scanOutput: Seq[Attribute],
    numSkippedSplits: SQLMetric)
  extends DataSourceRDD(sc, inputPartitions, partitionReaderFactory, columnarReads, customMetrics,
    None, checkedOutputWidth) {

  override protected def getDependencies: Seq[Dependency[_]] =
    Seq(new PerPartitionStorageFilterDependency(blooms, bloomPartitions))

  override protected def getPartitions: Array[Partition] = {
    val parents = blooms.partitions
    super.getPartitions.map {
      case p: DataSourceRDDPartition =>
        new PerPartitionStorageFilterPartition(
          p.index, p.inputPartition, bloomPartitions(p.index).map(parents))
    }
  }

  // The request each task built for the set of partitions of `blooms` it reads now. By task
  // attempt, as `DataSourceRDD` keeps its state.
  @transient private lazy val taskRequests =
    new ConcurrentHashMap[Long, mutable.HashMap[Seq[Int], Option[StorageFilterRequest]]]()

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val partition = split.asInstanceOf[PerPartitionStorageFilterPartition]
    if (partition.inputPartition.isEmpty) {
      Iterator.empty
    } else {
      requestOf(partition, context) match {
        case Some(request) => computeWith(split, context, Some(request))
        case None =>
          numSkippedSplits += 1
          Iterator.empty
      }
    }
  }

  // The request for the readers of `partition`, None when no bloom filter has its key.
  private def requestOf(
      partition: PerPartitionStorageFilterPartition,
      context: TaskContext): Option[StorageFilterRequest] = {
    val requests = taskRequests.computeIfAbsent(context.taskAttemptId(), _ => mutable.HashMap.empty)
    // Outside `computeIfAbsent`, whose function must not change the map. A task that has
    // completed runs the listener at once, and the listener removes the task's entry.
    if (requests.isEmpty) {
      context.addTaskCompletionListener[Unit](ctx => taskRequests.remove(ctx.taskAttemptId()))
    }
    // A task reads the splits of a key one after another, so it keeps only the request of the key
    // it reads now. So the requests a task keeps hold the bloom filters of one key, not those of
    // the whole creation side.
    val bloomKey = partition.bloomPartitions.map(_.index).toImmutableArraySeq
    if (!requests.contains(bloomKey)) requests.clear()
    requests.getOrElseUpdate(bloomKey, {
      val bytes = partition.bloomPartitions.toSeq.flatMap { p =>
        firstParent[InternalRow].iterator(p, context)
          .filterNot(_.isNullAt(bloomOrdinal))
          .map(_.getBinary(bloomOrdinal))
          .toSeq
      }
      merged(bytes).map { bloom =>
        val filter = condition.transformUp {
          case a: Attribute if a.exprId == bloomFilter.exprId => Literal(bloom, BinaryType)
        }
        StorageFilterRequestImpl(StorageFilterImpl(filter, scanOutput),
          checkedOutputWidth.isDefined)
      }
    })
  }

  // The bloom filters in `bytes` ORed into one. They have the same size, since the aggregate
  // builds each with the same expected items and bits.
  private def merged(bytes: Seq[Array[Byte]]): Option[Array[Byte]] = bytes match {
    case Seq() => None
    case Seq(only) => Some(only)
    case first +: rest =>
      val bloom = BloomFilterAggregate.deserialize(first)
      rest.foreach(b => bloom.mergeInPlace(BloomFilterAggregate.deserialize(b)))
      Some(BloomFilterAggregate.serialize(bloom))
  }
}

/** Each split of a [[PerPartitionStorageFilterRDD]] reads the partitions of its bloom filters. */
private class PerPartitionStorageFilterDependency(
    blooms: RDD[InternalRow],
    @transient private val bloomPartitions: Array[Array[Int]])
  extends NarrowDependency[InternalRow](blooms) {
  override def getParents(partitionId: Int): Seq[Int] =
    bloomPartitions(partitionId).toImmutableArraySeq
}
