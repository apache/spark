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

package org.apache.spark.deploy.yarn.history

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.scheduler._

case class TimestampEvent(sparkEvent: SparkListenerEvent, time: Long)

class YarnEventListener(sc: SparkContext, service: YarnHistoryService)
  extends SparkListener with Logging {

  /**
   * Called when a stage completes successfully or fails, with information on the completed stage.
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    service.enqueue(new TimestampEvent(stageCompleted, System.currentTimeMillis))
  }

  /**
   * Called when a stage is submitted
   */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    service.enqueue(new TimestampEvent(stageSubmitted, System.currentTimeMillis))
  }

  /**
   * Called when a task starts
   */
  override def onTaskStart(taskStart: SparkListenerTaskStart) {
    service.enqueue(new TimestampEvent(taskStart, System.currentTimeMillis))
  }

  /**
   * Called when a task begins remotely fetching its result (will not be called for tasks that do
   * not need to fetch the result remotely).
   */
  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
    service.enqueue(new TimestampEvent(taskGettingResult, System.currentTimeMillis))
  }

  /**
   * Called when a task ends
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    service.enqueue(new TimestampEvent(taskEnd, System.currentTimeMillis))
  }

  /**
   * Called when a job starts
   */
  override def onJobStart(jobStart: SparkListenerJobStart) {
    service.enqueue(new TimestampEvent(jobStart, System.currentTimeMillis))
  }


  /**
   * Called when a job ends
   */
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    service.enqueue(new TimestampEvent(jobEnd, System.currentTimeMillis))
  }

  /**
   * Called when environment properties have been updated
   */
  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    service.enqueue(new TimestampEvent(environmentUpdate, System.currentTimeMillis))
  }

  /**
   * Called when a new block manager has joined
   */
  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) {
    service.enqueue(new TimestampEvent(blockManagerAdded, System.currentTimeMillis))
  }

  /**
   * Called when an existing block manager has been removed
   */
  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) {
    service.enqueue(new TimestampEvent(blockManagerRemoved, System.currentTimeMillis))
  }

  /**
   * Called when an RDD is manually unpersisted by the application
   */
  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) {
    service.enqueue(new TimestampEvent(unpersistRDD, System.currentTimeMillis))
  }

  /**
   * Called when the application starts
   */
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    service.enqueue(new TimestampEvent(applicationStart, System.currentTimeMillis))
  }

  /**
   * Called when the application ends
   */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    service.enqueue(new TimestampEvent(applicationEnd, System.currentTimeMillis))
  }

  /**
   * Called when the driver receives task metrics from an executor in a heartbeat.
   */
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) {
    service.enqueue(new TimestampEvent(executorMetricsUpdate, System.currentTimeMillis))
  }
}
