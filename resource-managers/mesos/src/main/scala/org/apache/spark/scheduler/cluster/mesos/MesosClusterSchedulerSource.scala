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

import java.util.concurrent.TimeUnit
import java.util.Date

import scala.collection.mutable.HashMap

import com.codahale.metrics.{Counter, Gauge, MetricRegistry, Timer}
import org.apache.mesos.Protos.{TaskState => MesosTaskState}

import org.apache.spark.TaskState
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.metrics.source.Source

private[mesos] class MesosClusterSchedulerSource(scheduler: MesosClusterScheduler)
  extends Source with MesosSchedulerUtils {

  // Submission state transitions, to derive metrics from:
  // - submit():
  //     From: NULL
  //     To:   queuedDrivers
  // - offers/scheduleTasks():
  //     From: queuedDrivers and any pendingRetryDrivers scheduled for retry
  //     To:   launchedDrivers if success, or
  //           finishedDrivers(fail) if exception
  // - taskStatus/statusUpdate():
  //     From: launchedDrivers
  //     To:   finishedDrivers(success) if success (or fail and not eligible to retry), or
  //           pendingRetryDrivers if failed (and eligible to retry)
  // - pruning/retireDriver():
  //     From: finishedDrivers:
  //     To:   NULL

  override val sourceName: String = "mesos_cluster"
  override val metricRegistry: MetricRegistry = new MetricRegistry

  // PULL METRICS:
  // These gauge metrics are periodically polled/pulled by the metrics system

  metricRegistry.register(MetricRegistry.name("driver", "waiting"), new Gauge[Int] {
    override def getValue: Int = scheduler.getQueuedDriversSize
  })

  metricRegistry.register(MetricRegistry.name("driver", "launched"), new Gauge[Int] {
    override def getValue: Int = scheduler.getLaunchedDriversSize
  })

  metricRegistry.register(MetricRegistry.name("driver", "retry"), new Gauge[Int] {
    override def getValue: Int = scheduler.getPendingRetryDriversSize
  })

  metricRegistry.register(MetricRegistry.name("driver", "finished"), new Gauge[Int] {
    override def getValue: Int = scheduler.getFinishedDriversSize
  })

  // PUSH METRICS:
  // These metrics are updated directly as events occur

  private val queuedCounter = metricRegistry.counter(MetricRegistry.name("driver", "waiting_count"))
  private val launchedCounter =
    metricRegistry.counter(MetricRegistry.name("driver", "launched_count"))
  private val retryCounter = metricRegistry.counter(MetricRegistry.name("driver", "retry_count"))
  private val exceptionCounter =
    metricRegistry.counter(MetricRegistry.name("driver", "exception_count"))
  private val finishedCounter =
    metricRegistry.counter(MetricRegistry.name("driver", "finished_count"))

  // Same as finishedCounter above, except grouped by MesosTaskState.
  private val finishedMesosStateCounters = MesosTaskState.values
    // Avoid registering 'finished' metrics for states that aren't considered finished:
    .filter(state => TaskState.isFinished(mesosToTaskState(state)))
    .map(state => (state, metricRegistry.counter(
      MetricRegistry.name("driver", "finished_count_mesos_state", state.name.toLowerCase))))
    .toMap
  private val finishedMesosUnknownStateCounter =
    metricRegistry.counter(MetricRegistry.name("driver", "finished_count_mesos_state", "UNKNOWN"))

  // Duration from submission to FIRST launch.
  // This omits retries since those would exaggerate the time since original submission.
  private val submitToFirstLaunch =
    metricRegistry.timer(MetricRegistry.name("driver", "submit_to_first_launch"))
  // Duration from initial submission to an exception.
  private val submitToException =
    metricRegistry.timer(MetricRegistry.name("driver", "submit_to_exception"))

  // Duration from (most recent) launch to a retry.
  private val launchToRetry = metricRegistry.timer(MetricRegistry.name("driver", "launch_to_retry"))

  // Duration from initial submission to finished.
  private val submitToFinish =
    metricRegistry.timer(MetricRegistry.name("driver", "submit_to_finish"))
  // Duration from (most recent) launch to finished.
  private val launchToFinish =
    metricRegistry.timer(MetricRegistry.name("driver", "launch_to_finish"))

  // Same as submitToFinish and launchToFinish above, except grouped by Spark TaskState.
  class FinishStateTimers(state: String) {
    val submitToFinish =
      metricRegistry.timer(MetricRegistry.name("driver", "submit_to_finish_state", state))
    val launchToFinish =
      metricRegistry.timer(MetricRegistry.name("driver", "launch_to_finish_state", state))
  }
  private val finishSparkStateTimers = HashMap.empty[TaskState.TaskState, FinishStateTimers]
  for (state <- TaskState.values) {
    // Avoid registering 'finished' metrics for states that aren't considered finished:
    if (TaskState.isFinished(state)) {
      finishSparkStateTimers += (state -> new FinishStateTimers(state.toString.toLowerCase))
    }
  }
  private val submitToFinishUnknownState = metricRegistry.timer(
    MetricRegistry.name("driver", "submit_to_finish_state", "UNKNOWN"))
  private val launchToFinishUnknownState = metricRegistry.timer(
    MetricRegistry.name("driver", "launch_to_finish_state", "UNKNOWN"))

  // Histogram of retry counts at retry scheduling
  private val retryCount = metricRegistry.histogram(MetricRegistry.name("driver", "retry_counts"))

  // Records when a submission initially enters the launch queue.
  def recordQueuedDriver(): Unit = queuedCounter.inc

  // Records when a submission has failed an attempt and is eligible to be retried
  def recordRetryingDriver(state: MesosClusterSubmissionState): Unit = {
    state.driverDescription.retryState.foreach(retryState => retryCount.update(retryState.retries))
    recordTimeSince(state.startDate, launchToRetry)
    retryCounter.inc
  }

  // Records when a submission is launched.
  def recordLaunchedDriver(desc: MesosDriverDescription): Unit = {
    if (!desc.retryState.isDefined) {
      recordTimeSince(desc.submissionDate, submitToFirstLaunch)
    }
    launchedCounter.inc
  }

  // Records when a submission has successfully finished, or failed and was not eligible for retry.
  def recordFinishedDriver(state: MesosClusterSubmissionState, mesosState: MesosTaskState): Unit = {
    finishedCounter.inc

    recordTimeSince(state.driverDescription.submissionDate, submitToFinish)
    recordTimeSince(state.startDate, launchToFinish)

    // Timers grouped by Spark TaskState:
    val sparkState = mesosToTaskState(mesosState)
    finishSparkStateTimers.get(sparkState) match {
      case Some(timers) =>
        recordTimeSince(state.driverDescription.submissionDate, timers.submitToFinish)
        recordTimeSince(state.startDate, timers.launchToFinish)
      case None =>
        recordTimeSince(state.driverDescription.submissionDate, submitToFinishUnknownState)
        recordTimeSince(state.startDate, launchToFinishUnknownState)
    }

    // Counter grouped by MesosTaskState:
    finishedMesosStateCounters.get(mesosState) match {
      case Some(counter) => counter.inc
      case None => finishedMesosUnknownStateCounter.inc
    }
  }

  // Records when a submission has terminally failed due to an exception at construction.
  def recordExceptionDriver(desc: MesosDriverDescription): Unit = {
    recordTimeSince(desc.submissionDate, submitToException)
    exceptionCounter.inc
  }

  private def recordTimeSince(date: Date, timer: Timer): Unit =
    timer.update(System.currentTimeMillis - date.getTime, TimeUnit.MILLISECONDS)
}
