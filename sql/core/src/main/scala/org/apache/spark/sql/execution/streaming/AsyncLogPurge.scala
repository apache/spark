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

package org.apache.spark.sql.execution.streaming

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ThreadUtils

/**
 * Used to enable the capability to allow log purges to be done asynchronously
 */
trait AsyncLogPurge extends Logging {

  protected val minLogEntriesToMaintain: Int

  protected[sql] val errorNotifier: ErrorNotifier

  protected val sparkSession: SparkSession

  private val asyncPurgeExecutorService
    = ThreadUtils.newDaemonSingleThreadExecutor("async-log-purge")

  private val purgeRunning = new AtomicBoolean(false)

  private val statefulMetadataPurgeRunning = new AtomicBoolean(false)

  protected def purge(threshold: Long): Unit

  // This method is used to purge the oldest OperatorStateMetadata and StateSchema files
  // which are written per run.
  protected def purgeStatefulMetadata(plan: SparkPlan): Unit

  protected lazy val useAsyncPurge: Boolean = sparkSession.sessionState.conf
    .getConf(SQLConf.ASYNC_LOG_PURGE)

  protected def purgeAsync(batchId: Long): Unit = {
    if (purgeRunning.compareAndSet(false, true)) {
      asyncPurgeExecutorService.execute(() => {
        try {
          purge(batchId - minLogEntriesToMaintain)
        } catch {
          case throwable: Throwable =>
            logError("Encountered error while performing async log purge", throwable)
            errorNotifier.markError(throwable)
        } finally {
          purgeRunning.set(false)
        }
      })
    } else {
      log.debug("Skipped log purging since there is already one in progress.")
    }
  }

  protected def purgeStatefulMetadataAsync(plan: SparkPlan): Unit = {
    if (statefulMetadataPurgeRunning.compareAndSet(false, true)) {
      asyncPurgeExecutorService.execute(() => {
        try {
          purgeStatefulMetadata(plan)
        } catch {
          case throwable: Throwable =>
            logError("Encountered error while performing async log purge", throwable)
            errorNotifier.markError(throwable)
        } finally {
          statefulMetadataPurgeRunning.set(false)
        }
      })
    } else {
      log.debug("Skipped log purging since there is already one in progress.")
    }
  }

  protected def asyncLogPurgeShutdown(): Unit = {
    ThreadUtils.shutdown(asyncPurgeExecutorService)
  }

  // used for testing
  private[sql] def arePendingAsyncPurge: Boolean = {
    purgeRunning.get() ||
      asyncPurgeExecutorService.getQueue.size() > 0 ||
      asyncPurgeExecutorService.getActiveCount > 0
  }
}
