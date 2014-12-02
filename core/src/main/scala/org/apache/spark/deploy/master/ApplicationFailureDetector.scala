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

package org.apache.spark.deploy.master

import org.apache.spark.Logging

/**
 * This class encapsulates the Worker's logic for deciding whether to kill a Spark application
 * based on executor failures.  The goal of this logic is to ensure that buggy applications are
 * killed in a prompt manner, while preventing applications from being killed due to buggy
 * machines / executors.
 *
 * Thread-safety: this class is not thread-safe because it is only intended to be called
 * from the Master actor.
 */
private[master] class ApplicationFailureDetector(
    appName: String,
    appId: String)
  extends Logging with Serializable {

  private val MAX_CONSECUTIVE_FAILURES = 10

  private var consecutiveExecutorFailures = 0

  /**
   * True if the driver has reported that it has at least one registered executor, false otherwise.
   */
  private var hasRegisteredExecutors: Boolean = false

  /**
   * Called when an application's executor status might have changed.
   */
  def updateExecutorStatus(hasRegisteredExecutors: Boolean): Unit = {
    this.hasRegisteredExecutors = hasRegisteredExecutors
    if (hasRegisteredExecutors) {
      consecutiveExecutorFailures = 0
    }
  }

  /**
   * Called when an executor exits due to a failure.
   */
  def onFailedExecutorExit(execId: Int): Unit = {
    consecutiveExecutorFailures += 1
  }

  /**
   * Ask the failure detector whether the application should be marked as failed.
   *
   * @return true if the application has failed, false otherwise.
   */
  def isFailed: Boolean = {
    if (consecutiveExecutorFailures >= MAX_CONSECUTIVE_FAILURES && !hasRegisteredExecutors) {
      logError(s"Application $appName with ID $appId is failed because it has no executors and " +
        s"there were $consecutiveExecutorFailures consecutive executor failures")
      true
    } else {
      false
    }
  }
}
