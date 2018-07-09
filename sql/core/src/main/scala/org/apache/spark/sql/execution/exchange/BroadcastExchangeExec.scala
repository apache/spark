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

import java.util.concurrent.TimeoutException

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

import org.apache.spark.{broadcast, SparkException}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.joins.HashedRelation
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.{SparkFatalException, ThreadUtils}

/**
 * A [[BroadcastExchangeExec]] collects, transforms and finally broadcasts the result of
 * a transformed SparkPlan.
 */
case class BroadcastExchangeExec(
    mode: BroadcastMode,
    child: SparkPlan) extends Exchange {

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
    "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)"),
    "buildTime" -> SQLMetrics.createMetric(sparkContext, "time to build (ms)"),
    "broadcastTime" -> SQLMetrics.createMetric(sparkContext, "time to broadcast (ms)"))

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  override def doCanonicalize(): SparkPlan = {
    BroadcastExchangeExec(mode.canonicalized, child.canonicalized)
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

  @transient
  private lazy val relationFuture: Future[broadcast.Broadcast[Any]] = {
    // broadcastFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sqlContext.sparkSession, executionId) {
        try {
          val beforeCollect = System.nanoTime()
          // Use executeCollect/executeCollectIterator to avoid conversion to Scala types
          val (numRows, input) = child.executeCollectIterator()
          if (numRows >= 512000000) {
            throw new SparkException(
              s"Cannot broadcast the table with more than 512 millions rows: $numRows rows")
          }

          val beforeBuild = System.nanoTime()
          longMetric("collectTime") += (beforeBuild - beforeCollect) / 1000000

          // Construct the relation.
          val relation = mode.transform(input, Some(numRows))

          val dataSize = relation match {
            case map: HashedRelation =>
              map.estimatedSize
            case arr: Array[InternalRow] =>
              arr.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
            case _ =>
              throw new SparkException("[BUG] BroadcastMode.transform returned unexpected type: " +
                  relation.getClass.getName)
          }

          longMetric("dataSize") += dataSize
          if (dataSize >= (8L << 30)) {
            throw new SparkException(
              s"Cannot broadcast the table that is larger than 8GB: ${dataSize >> 30} GB")
          }

          val beforeBroadcast = System.nanoTime()
          longMetric("buildTime") += (beforeBroadcast - beforeBuild) / 1000000

          // Broadcast the relation
          val broadcasted = sparkContext.broadcast(relation)
          longMetric("broadcastTime") += (System.nanoTime() - beforeBroadcast) / 1000000

          SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
          broadcasted
        } catch {
          // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
          // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
          // will catch this exception and re-throw the wrapped fatal throwable.
          case oe: OutOfMemoryError =>
            throw new SparkFatalException(
              new OutOfMemoryError(s"Not enough memory to build and broadcast the table to " +
              s"all worker nodes. As a workaround, you can either disable broadcast by setting " +
              s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark driver " +
              s"memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value")
              .initCause(oe.getCause))
          case e if !NonFatal(e) =>
            throw new SparkFatalException(e)
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
    try {
      ThreadUtils.awaitResult(relationFuture, timeout).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in ${timeout.toSeconds} secs.", ex)
        throw new SparkException(s"Could not execute broadcast in ${timeout.toSeconds} secs. " +
          s"You can increase the timeout for broadcasts via ${SQLConf.BROADCAST_TIMEOUT.key} or " +
          s"disable broadcast join by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1",
          ex)
    }
  }
}

object BroadcastExchangeExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("broadcast-exchange", 128))
}
