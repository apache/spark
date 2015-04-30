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

/**
 * A backend interface for scheduling systems that allows plugging in different ones under
 * TaskSchedulerImpl. We assume a Mesos-like model where the application gets resource offers as
 * machines become available and can launch tasks on them.
 */
private[spark] trait SchedulerBackend {
  private val appId = "spark-application-" + System.currentTimeMillis

  def start(): Unit
  def stop(): Unit
  def reviveOffers(): Unit
  def defaultParallelism(): Int

  def killTask(taskId: Long, executorId: String, interruptThread: Boolean): Unit =
    throw new UnsupportedOperationException
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

}
