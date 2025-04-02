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

package org.apache.spark.scheduler

import org.apache.spark.resource.ResourceProfile
import org.apache.spark.status.api.v1.ThreadStackTrace
import org.apache.spark.storage.BlockManagerId

/**
 * A backend interface for scheduling systems that allows plugging in different ones under
 * TaskSchedulerImpl. We assume a model where the application gets resource offers as
 * machines become available and can launch tasks on them.
 */
private[spark] trait SchedulerBackend {
  private val appId = "spark-application-" + System.currentTimeMillis

  def start(): Unit
  def stop(): Unit
  def stop(exitCode: Int): Unit = stop()
  def updateExecutorsLogLevel(logLevel: String): Unit = {}
  /**
   * Update the current offers and schedule tasks
   */
  def reviveOffers(): Unit
  def defaultParallelism(): Int

  /**
   * Requests that an executor kills a running task.
   *
   * @param taskId Id of the task.
   * @param executorId Id of the executor the task is running on.
   * @param interruptThread Whether the executor should interrupt the task thread.
   * @param reason The reason for the task kill.
   */
  def killTask(
      taskId: Long,
      executorId: String,
      interruptThread: Boolean,
      reason: String): Unit =
    throw new UnsupportedOperationException

  def getTaskThreadDump(taskId: Long, executorId: String): Option[ThreadStackTrace]

  def isReady(): Boolean = true

  /**
   * Get an application ID associated with the job.
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Get the attempt ID for this run, if the cluster manager supports multiple
   * attempts. Applications run in client mode will not have attempt IDs.
   *
   * @return The application attempt id, if available.
   */
  def applicationAttemptId(): Option[String] = None

  /**
   * Get the URLs for the driver logs. These URLs are used to display the links in the UI
   * Executors tab for the driver.
   * @return Map containing the log names and their respective URLs
   */
  def getDriverLogUrls: Option[Map[String, String]] = None

  /**
   * Get the attributes on driver. These attributes are used to replace log URLs when
   * custom log url pattern is specified.
   * @return Map containing attributes on driver.
   */
  def getDriverAttributes: Option[Map[String, String]] = None

  /**
   * Get the max number of tasks that can be concurrent launched based on the ResourceProfile
   * could be used, even if some of them are being used at the moment.
   * Note that please don't cache the value returned by this method, because the number can change
   * due to add/remove executors.
   *
   * @param rp ResourceProfile which to use to calculate max concurrent tasks.
   * @return The max number of tasks that can be concurrent launched currently.
   */
  def maxNumConcurrentTasks(rp: ResourceProfile): Int

  /**
   * Get the list of host locations for push based shuffle
   *
   * Currently push based shuffle is disabled for both stage retry and stage reuse cases
   * (for eg: in the case where few partitions are lost due to failure). Hence this method
   * should be invoked only once for a ShuffleDependency.
   * @return List of external shuffle services locations
   */
  def getShufflePushMergerLocations(
      numPartitions: Int,
      resourceProfileId: Int): Seq[BlockManagerId] = Nil

}
