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

package org.apache.spark.scheduler.cluster.mesos

import org.apache.mesos.Protos.TaskStatus
import java.util.Date
import org.apache.spark.deploy.mesos.MesosDriverDescription

import scala.collection.mutable.ArrayBuffer

/**
 * Tracks the retry state of a driver, which includes the next time it should be scheduled
 * and necessary information to do exponential backoff.
 * @param submission Driver submission.
 * @param lastFailureStatus Last Task status when it failed.
 * @param retries Number of times it has retried.
 * @param nextRetry Next retry time to be scheduled.
 * @param waitTime The amount of time driver is scheduled to wait until next retry.
 */
private[spark] case class RetryState(
    submission: MesosDriverDescription,
    lastFailureStatus: TaskStatus,
    retries: Int,
    nextRetry: Date,
    waitTime: Int) extends Serializable {
  def copy() = new RetryState(submission, lastFailureStatus, retries, nextRetry, waitTime)
}

/**
 * Tracks all the drivers that were submitted under supervise which failed to run
 * and waiting to be scheduled again.
 * @param state Persistence engine to store state.
 */
private[mesos] class SuperviseRetryList(state: MesosClusterPersistenceEngine) {
  val drivers = new ArrayBuffer[RetryState]

  initialize()

  def initialize(): Unit = {
    state.fetchAll[RetryState]().foreach(drivers.+=)
  }

  def contains(submissionId: String): Boolean =
    drivers.exists(d => d.submission.submissionId.equals(submissionId))

  def getNextRetry(currentTime: Date): (Option[MesosDriverDescription], Option[RetryState]) = {
    val retry = drivers.find(d => d.nextRetry.before(currentTime))
    if (retry.isDefined) {
      (Some(retry.get.submission), retry)
    } else {
      (None, None)
    }
  }

  def get(submissionId: String): Option[RetryState] = {
    drivers.find(d => d.submission.submissionId.equals(submissionId))
  }

  def retries: Iterable[RetryState] = {
    drivers.map(d => d.copy).toList
  }

  def remove(submissionId: String): Boolean = {
    val index = drivers.indexWhere(s => s.submission.submissionId.equals(submissionId))

    if (index != -1) {
      drivers.remove(index)
      state.expunge(submissionId)
    }

    index != -1
  }

  def add(retryState: RetryState): Unit = {
    drivers += retryState
    state.persist(retryState.submission.submissionId.get, retryState)
  }
}
