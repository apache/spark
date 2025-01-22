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

package org.apache.spark.sql.execution.streaming.continuous

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.write.PhysicalWriteInfoImpl
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * The physical plan for writing data into a continuous processing [[StreamingWrite]].
 */
case class WriteToContinuousDataSourceExec(write: StreamingWrite, query: SparkPlan,
    customMetrics: Seq[CustomMetric])
  extends UnaryExecNode with Logging {

  override def child: SparkPlan = query
  override def output: Seq[Attribute] = Nil

  override lazy val metrics = customMetrics.map { customMetric =>
    customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
  }.toMap

  override protected def doExecute(): RDD[InternalRow] = {
    val queryRdd = query.execute()
    val writerFactory = write.createStreamingWriterFactory(
      PhysicalWriteInfoImpl(queryRdd.getNumPartitions))
    val rdd = new ContinuousWriteRDD(queryRdd, writerFactory, metrics)

    logInfo(log"Start processing data source write support: ${MDC(STREAMING_WRITE, write)}. " +
      log"The input RDD has ${MDC(NUM_PARTITIONS, rdd.partitions.length)} partitions.")
    EpochCoordinatorRef.get(
      sparkContext.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY),
      sparkContext.env)
      .askSync[Unit](SetWriterPartitions(rdd.getNumPartitions))

    try {
      // Force the RDD to run so continuous processing starts; no data is actually being collected
      // to the driver, as ContinuousWriteRDD outputs nothing.
      rdd.collect()
    } catch {
      case _: InterruptedException =>
        // Interruption is how continuous queries are ended, so accept and ignore the exception.
    }

    sparkContext.emptyRDD
  }

  override protected def withNewChildInternal(
    newChild: SparkPlan): WriteToContinuousDataSourceExec = copy(query = newChild)
}
