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

package org.apache.spark.streaming.scheduler

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.scheduler.batch.BatchEstimator
import org.apache.spark.util.ThreadUtils
import scala.concurrent.{Future ,ExecutionContext}

abstract class BatchController(val streamUID: Int, batchEstimator: BatchEstimator)
  extends StreamingListener with Serializable with Logging {

  init()

  protected def publish(rate: Long): Unit

  @transient
  implicit private var executionContext: ExecutionContext = _

  @transient
  private var batchInterval: AtomicLong = _

  var totalDelay: Long = -1L
  var schedulerDelay: Long = -1L
  var numRecords: Long = -1L
  var processingDelay: Long = -1L
  var batchIntevl: Long = -1L

  /**
    * An initialization method called both from the constructor and Serialization code.
    */
  private def init() {
    executionContext = ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonSingleThreadExecutor("stream-batchInteval-update"))
    batchInterval = new AtomicLong(-1L)
  }

  /**
    * Computing the Batch Intarvel and Publish it
    */
  private def computeAndPublish(processTime: Long, batchIntevl: Long): Unit =
    Future[Unit]{
      val newBatchInterval = batchEstimator.compute(processTime, batchIntevl)
      logInfo(s" #####  the newBatchInterval is $newBatchInterval")
      newBatchInterval.foreach{ s =>
        batchInterval.set(s)
        logInfo(s" ##### after setting newBatchInterval is $batchInterval")
        publish(getLatestBatchInterval())
      }
    }

  def getLatestBatchInterval(): Long = batchInterval.get()


  /**
    * Compute the batch interval after completed
    * @param batchCompleted
    */
  override def onBatchCompleted (batchCompleted: StreamingListenerBatchCompleted) {
    totalDelay = batchCompleted.batchInfo.totalDelay.get
    schedulerDelay = batchCompleted.batchInfo.schedulingDelay.get
    numRecords = batchCompleted.batchInfo.numRecords
    processingDelay = batchCompleted.batchInfo.processingDelay.get
    batchIntevl = batchCompleted.batchInfo.batchInterval

    logInfo(s"processingDelay $processingDelay | batchInterval $batchIntevl " +
      s"| totalDelay $totalDelay | schedulerDelay $schedulerDelay | numRecords $numRecords")

    for{
      processingTime <- batchCompleted.batchInfo.processingDelay
      batchInterval <- Option(batchCompleted.batchInfo.batchInterval)
    } computeAndPublish(processingTime, batchInterval)

    logInfo(s" ##### Hear onBatchCompleted msg and begin to compute.")
  }
}
  /**
    *Get the configure
    */
object BatchController {
    // is the dynamic batch interval enabled
    var isEnable: Boolean = false
    def isDynamicBatchIntervalEnabled(conf: SparkConf): Boolean =
      conf.getBoolean("spark.streaming.dynamicBatchInterval.enabled", false)

    def getBatchIntervalEnabled(): Boolean = isEnable
    def setBatchIntervalEnabled(enabled: Boolean): Unit = {
      isEnable = enabled
    }
}
