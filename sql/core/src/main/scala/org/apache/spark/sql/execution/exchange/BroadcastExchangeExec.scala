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

package org.apache.spark.sql.execution.exchange

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag

import org.apache.spark.{broadcast, SparkException}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastPartitioning, Partitioning, RowBroadcastMode}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.joins.HashedRelation
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ThreadUtils

/**
 * A [[BroadcastExchangeExec]] collects, transforms and finally broadcasts the result of
 * a transformed SparkPlan.
 *
 * @tparam T The type of the object transformed from the result of RDD by [[BroadcastMode]].
 */
case class BroadcastExchangeExec[T: ClassTag](
    mode: RowBroadcastMode,
    child: SparkPlan) extends Exchange {

  override lazy val metrics = if (sqlContext.conf.executorSideBroadcastEnabled) {
      Map(
        "buildTime" -> SQLMetrics.createMetric(sparkContext, "time to build (ms)"),
        "broadcastTime" -> SQLMetrics.createMetric(sparkContext, "time to broadcast (ms)"))
    } else {
      Map(
        "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
        "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)"),
        "buildTime" -> SQLMetrics.createMetric(sparkContext, "time to build (ms)"),
        "broadcastTime" -> SQLMetrics.createMetric(sparkContext, "time to broadcast (ms)"))
    }

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  override def sameResult(plan: SparkPlan): Boolean = plan match {
    case p: BroadcastExchangeExec[_] =>
      mode.compatibleWith(p.mode) && child.sameResult(p.child)
    case _ => false
  }

  @transient
  private val timeout: Duration = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  // Private variable used to hold the reference of RDD created during executor-side broadcasting.
  // If we don't keep its reference, it will be cleaned up.
  private var childRDD: RDD[InternalRow] = null

  private def executorSideBroadcast(): broadcast.Broadcast[Any] = {
    val beforeBuild = System.nanoTime()
    // Call persist on the RDD because we want to broadcast the RDD blocks on executors.
    childRDD = child.execute().mapPartitionsInternal { rowIterator =>
      rowIterator.map(_.copy())
    }.persist(StorageLevel.MEMORY_AND_DISK)

    val numOfRows = childRDD.count()
    if (numOfRows >= 512000000) {
      throw new SparkException(
        s"Cannot broadcast the table with more than 512 millions rows: ${numOfRows} rows")
    }

    // Broadcast the relation on executors.
    val beforeBroadcast = System.nanoTime()
    longMetric("buildTime") += (beforeBuild - beforeBroadcast) / 1000000

    val broadcasted = sparkContext.broadcastRDDOnExecutor[InternalRow, T](childRDD, mode)
      .asInstanceOf[broadcast.Broadcast[Any]]

    longMetric("broadcastTime") += (System.nanoTime() - beforeBroadcast) / 1000000
    broadcasted
  }

  private def driverSideBroadcast(): broadcast.Broadcast[Any] = {
    val beforeCollect = System.nanoTime()
    // Note that we use .executeCollect() because we don't want to convert data to
    // Scala types
    val input: Array[InternalRow] = child.executeCollect()
    if (input.length >= 512000000) {
      throw new SparkException(
        s"Cannot broadcast the table with more than 512 millions rows: ${input.length} rows")
    }
    val beforeBuild = System.nanoTime()
    longMetric("collectTime") += (beforeBuild - beforeCollect) / 1000000
    val dataSize = input.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
    longMetric("dataSize") += dataSize
    if (dataSize >= (8L << 30)) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than 8GB: ${dataSize >> 30} GB")
    }
    // Construct and broadcast the relation.
    val relation = mode.transform(input)
    val beforeBroadcast = System.nanoTime()
    longMetric("buildTime") += (beforeBroadcast - beforeBuild) / 1000000
    val broadcasted = sparkContext.broadcast(relation)
    longMetric("broadcastTime") += (System.nanoTime() - beforeBroadcast) / 1000000
    broadcasted
  }

  @transient
  private lazy val relationFuture: Future[broadcast.Broadcast[Any]] = {
    // broadcastFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sparkContext, executionId) {
        try {
          val broadcasted = if (sqlContext.conf.executorSideBroadcastEnabled) {
            executorSideBroadcast()
          } else {
            driverSideBroadcast()
          }

          // There are some cases we don't care about the metrics and call `SparkPlan.doExecute`
          // directly without setting an execution id. We should be tolerant to it.
          if (executionId != null) {
            sparkContext.listenerBus.post(SparkListenerDriverAccumUpdates(
              executionId.toLong, metrics.values.map(m => m.id -> m.value).toSeq))
          }

          broadcasted
        } catch {
          case oe: OutOfMemoryError =>
            throw new OutOfMemoryError(s"Not enough memory to build and broadcast the table to " +
              s"all worker nodes. As a workaround, you can either disable broadcast by setting " +
              s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark driver " +
              s"memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value")
              .initCause(oe.getCause)
        }
      }
    }(BroadcastExchangeExec.executionContext)
  }

  override protected def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    ThreadUtils.awaitResult(relationFuture, timeout).asInstanceOf[broadcast.Broadcast[T]]
  }

  override protected def otherCopyArgs: Seq[AnyRef] = Seq(implicitly[ClassTag[T]])
}

object BroadcastExchangeExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("broadcast-exchange", 128))
}
