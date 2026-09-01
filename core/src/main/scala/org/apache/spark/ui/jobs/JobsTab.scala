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

package org.apache.spark.ui.jobs

import java.util.concurrent.{ExecutorService, RejectedExecutionException}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.config.SCHEDULER_MODE
import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui._
import org.apache.spark.util.{ThreadUtils, Utils}

/** Web UI showing progress status of all jobs in the given SparkContext. */
private[ui] class JobsTab(parent: SparkUI, store: AppStatusStore)
  extends SparkUITab(parent, "jobs") {

  val sc = parent.sc
  val conf = parent.conf
  val killEnabled = parent.killEnabled
  val holdEnabled = parent.holdEnabled

  // Show pool information for only live UI.
  def isFairScheduler: Boolean = {
    sc.isDefined &&
    store
      .environmentInfo()
      .sparkProperties
      .contains((SCHEDULER_MODE.key, SchedulingMode.FAIR.toString))
  }

  def getSparkUser: String = parent.getSparkUser

  attachPage(new AllJobsPage(this, store))
  attachPage(new JobPage(this, store))

  def handleKillRequest(request: HttpServletRequest): Unit = {
    if (killEnabled && parent.securityManager.checkModifyPermissions(request.getRemoteUser)) {
      Option(request.getParameter("id")).map(_.toInt).foreach { id =>
        store.asOption(store.job(id)).foreach { job =>
          if (job.status == JobExecutionStatus.RUNNING) {
            sc.foreach(_.cancelJob(id, "killed via Web UI"))
            // Do a quick pause here to give Spark time to kill the job so it shows up as
            // killed after the refresh. Note that this will block the serving thread so the
            // time should be limited in duration.
            Thread.sleep(100)
          }
        }
      }
    }
  }

  // Serves the hold/resume requests off the Jetty serving thread: they talk to the cluster
  // manager and may block up to the RPC ask timeout. A single thread also serializes
  // concurrent requests. Created on first use and shut down by `stop()`, so that the thread
  // does not outlive the SparkContext.
  private var holdRequestExecutor: Option[ExecutorService] = None
  private var stopped = false

  // None once stopped, so that a request served during teardown neither hits a rejected
  // execution on the shut-down pool nor recreates it and leaks the thread.
  private def holdRequestExecutorPool: Option[ExecutorService] = synchronized {
    if (stopped) {
      None
    } else {
      Some(holdRequestExecutor.getOrElse {
        val pool = ThreadUtils.newDaemonSingleThreadExecutor("spark-ui-hold-resume")
        holdRequestExecutor = Some(pool)
        pool
      })
    }
  }

  def stop(): Unit = synchronized {
    stopped = true
    holdRequestExecutor.foreach(_.shutdownNow())
  }

  // Outcome of the last hold/resume request served by this tab, as (isHold, message): Some
  // while a request is running or after it did not take effect, None when idle or after a
  // success. Rendered with the operation it belongs to, so a stale message never reads as
  // the opposite operation's, and a failed hold stays visible even though the page already
  // shows the (resume) control (the hold is marked before the cluster manager is asked).
  @volatile private var holdRequestStatus: Option[(Boolean, String)] = None

  private[jobs] def lastHoldRequestStatus: Option[String] =
    holdRequestStatus.map { case (isHold, message) =>
      s"(${if (isHold) "hold" else "resume"}: $message)"
    }

  def handleHoldRequest(request: HttpServletRequest): Unit = {
    if (holdEnabled && parent.securityManager.checkModifyPermissions(request.getRemoteUser)) {
      sc.filter(_.executorHoldSupported).foreach { ctx =>
        holdRequestExecutorPool.foreach { pool =>
          holdRequestStatus = Some((true, "requested"))
          try {
            pool.execute { () =>
              var acknowledged = false
              Utils.tryLogNonFatalError { acknowledged = ctx.holdExecutors() }
              holdRequestStatus = if (acknowledged) {
                None
              } else {
                Some((true, "the last request did not take effect, see the driver logs"))
              }
            }
          } catch {
            // stop() may have shut the pool down after the accessor returned it
            case _: RejectedExecutionException =>
          }
        }
      }
    }
  }

  def handleResumeRequest(request: HttpServletRequest): Unit = {
    if (holdEnabled && parent.securityManager.checkModifyPermissions(request.getRemoteUser)) {
      sc.filter(_.executorHoldSupported).foreach { ctx =>
        holdRequestExecutorPool.foreach { pool =>
          holdRequestStatus = Some((false, "requested"))
          try {
            pool.execute { () =>
              var acknowledged = false
              Utils.tryLogNonFatalError { acknowledged = ctx.resumeExecutors() }
              holdRequestStatus = if (acknowledged) {
                None
              } else {
                Some((false, "the last request did not take effect, see the driver logs"))
              }
            }
          } catch {
            // stop() may have shut the pool down after the accessor returned it
            case _: RejectedExecutionException =>
          }
        }
      }
    }
  }
}
