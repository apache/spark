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

import java.io.ObjectInputStream
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingConf.BACKPRESSURE_ENABLED
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * A StreamingListener that receives batch completion updates, and maintains
 * an estimate of the speed at which this stream should ingest messages,
 * given an estimate computation from a `RateEstimator`
 */
private[streaming] abstract class RateController(val streamUID: Int, rateEstimator: RateEstimator)
    extends StreamingListener with Serializable {

  init()

  protected def publish(rate: Long): Unit

  @transient
  implicit private var executionContext: ExecutionContext = _

  @transient
  private var rateLimit: AtomicLong = _

  /**
   * An initialization method called both from the constructor and Serialization code.
   */
  private def init(): Unit = {
    executionContext = ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonSingleThreadExecutor("stream-rate-update"))
    rateLimit = new AtomicLong(-1L)
  }

  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    ois.defaultReadObject()
    init()
  }

  /**
   * Compute the new rate limit and publish it asynchronously.
   */
  private def computeAndPublish(time: Long, elems: Long, workDelay: Long, waitDelay: Long): Unit =
    Future[Unit] {
      val newRate = rateEstimator.compute(time, elems, workDelay, waitDelay)
      newRate.foreach { s =>
        rateLimit.set(s.toLong)
        publish(getLatestRate())
      }
    }

  def getLatestRate(): Long = rateLimit.get()

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val elements = batchCompleted.batchInfo.streamIdToInputInfo

    for {
      processingEnd <- batchCompleted.batchInfo.processingEndTime
      workDelay <- batchCompleted.batchInfo.processingDelay
      waitDelay <- batchCompleted.batchInfo.schedulingDelay
      elems <- elements.get(streamUID).map(_.numRecords)
    } computeAndPublish(processingEnd, elems, workDelay, waitDelay)
  }
}

object RateController {
  def isBackPressureEnabled(conf: SparkConf): Boolean =
    conf.get(BACKPRESSURE_ENABLED)
}
