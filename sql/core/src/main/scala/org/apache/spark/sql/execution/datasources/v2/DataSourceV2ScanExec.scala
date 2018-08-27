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
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ColumnarBatchScan, LeafExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming._

/**
 * Physical plan node for scanning data from a data source.
 */
case class DataSourceV2ScanExec(
    output: Seq[Attribute],
    @transient source: DataSourceV2,
    @transient options: Map[String, String],
    @transient pushedFilters: Seq[Expression],
    @transient scan: Scan,
    // `ProgressReporter` needs to know which stream a physical scan node associates to, so that
    // it can collect metrics for a stream correctly.
    @transient stream: Option[InputStream] = None)
  extends LeafExecNode with DataSourceV2StringFormat with ColumnarBatchScan {

  override def simpleString: String = "ScanV2 " + metadataString

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: DataSourceV2ScanExec =>
      output == other.output && source.getClass == other.source.getClass && options == other.options
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(output, source.getClass, options).hashCode()
  }

  override def outputPartitioning: physical.Partitioning = scan match {
    case _ if partitions.length == 1 =>
      SinglePartition

    case s: SupportsReportPartitioning =>
      new DataSourcePartitioning(s.outputPartitioning(), AttributeMap(output.map(a => a -> a.name)))

    case _ => super.outputPartitioning
  }

  private lazy val partitions: Seq[InputPartition] = scan.planInputPartitions()

  private lazy val readerFactory = scan match {
    case scan: BatchScan => scan.createReaderFactory()
    case scan: MicroBatchScan => scan.createReaderFactory()
    case scan: ContinuousScan => scan.createContinuousReaderFactory()
    case _ => throw new IllegalStateException("unknown read support: " + scan)
  }

  // TODO: clean this up when we have dedicated scan plan for continuous streaming.
  override val supportsBatch: Boolean = {
    require(partitions.forall(readerFactory.supportColumnarReads) ||
      !partitions.exists(readerFactory.supportColumnarReads),
      "Cannot mix row-based and columnar input partitions.")

    partitions.exists(readerFactory.supportColumnarReads)
  }

  private lazy val inputRDD: RDD[InternalRow] = scan match {
    case _: ContinuousScan =>
      assert(!supportsBatch,
        "continuous stream reader does not support columnar read yet.")
      EpochCoordinatorRef.get(
          sparkContext.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY),
          sparkContext.env)
        .askSync[Unit](SetReaderPartitions(partitions.size))
      new ContinuousDataSourceRDD(
        sparkContext,
        sqlContext.conf.continuousStreamingExecutorQueueSize,
        sqlContext.conf.continuousStreamingExecutorPollIntervalMs,
        partitions,
        schema,
        readerFactory.asInstanceOf[ContinuousPartitionReaderFactory])

    case _ =>
      new DataSourceRDD(
        sparkContext, partitions, readerFactory.asInstanceOf[PartitionReaderFactory], supportsBatch)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD)

  override protected def needsUnsafeRowConversion: Boolean = false

  override protected def doExecute(): RDD[InternalRow] = {
    if (supportsBatch) {
      WholeStageCodegenExec(this)(codegenStageId = 0).execute()
    } else {
      val numOutputRows = longMetric("numOutputRows")
      inputRDD.map { r =>
        numOutputRows += 1
        r
      }
    }
  }
}
