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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.connector.read.streaming.{ContinuousPartitionReaderFactory, ContinuousStream, Offset}
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for scanning data from a streaming data source with continuous mode.
 */
case class ContinuousScanExec(
    output: Seq[Attribute],
    @transient scan: Scan,
    @transient stream: ContinuousStream,
    @transient start: Offset,
    keyGroupedPartitioning: Option[Seq[Expression]] = None,
    ordering: Option[Seq[SortOrder]] = None) extends DataSourceV2ScanExecBase {

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: ContinuousScanExec => this.stream == other.stream
    case _ => false
  }

  override def hashCode(): Int = stream.hashCode()

  override lazy val inputPartitions: Seq[InputPartition] =
    stream.planInputPartitions(start).toImmutableArraySeq

  override lazy val readerFactory: ContinuousPartitionReaderFactory = {
    stream.createContinuousReaderFactory()
  }

  override lazy val inputRDD: RDD[InternalRow] = {
    assert(partitions.forall(_.length == 1), "should only contain a single partition")
    EpochCoordinatorRef.get(
      sparkContext.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY),
      sparkContext.env)
      .askSync[Unit](SetReaderPartitions(partitions.size))
    val inputRDD = new ContinuousDataSourceRDD(
      sparkContext,
      conf.continuousStreamingExecutorQueueSize,
      conf.continuousStreamingExecutorPollIntervalMs,
      partitions.map(_.head),
      schema,
      readerFactory,
      customMetrics)
    postDriverMetrics()
    inputRDD
  }
}
