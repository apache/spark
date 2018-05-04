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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.Date

import scala.collection.mutable.HashMap

import com.codahale.metrics.{Counter, Gauge, MetricRegistry, Timer}
import org.apache.mesos.Protos.{TaskState => MesosTaskState}

import org.apache.spark.TaskState
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.metrics.source.Source

private[mesos] class MesosCoarseGrainedSchedulerSource(
  scheduler: MesosCoarseGrainedSchedulerBackend)
    extends Source with MesosSchedulerUtils {

  override val sourceName: String = "mesos_cluster"
  override val metricRegistry: MetricRegistry = new MetricRegistry

  // EXECUTOR STATE POLLING METRICS:
  // These metrics periodically poll the scheduler for its state, including resource allocation and
  // task states.

  // Number of CPUs used
  metricRegistry.register(MetricRegistry.name("executor", "resource", "cores"), new Gauge[Double] {
    override def getValue: Double = scheduler.getCoresUsed
  })
  // Number of CPUs vs max
  if (scheduler.getMaxCores != 0) {
    metricRegistry.register(MetricRegistry.name("executor", "resource", "cores_of_max"),
      new Gauge[Double] {
        // Note: See above div0 check before calling register()
        override def getValue: Double = scheduler.getCoresUsed / scheduler.getMaxCores
      })
  }
  // Number of CPUs per task
  metricRegistry.register(MetricRegistry.name("executor", "resource", "mean_cores_per_task"),
    new Gauge[Double] {
      override def getValue: Double = scheduler.getMeanCoresPerTask
    })

  // Number of GPUs used
  metricRegistry.register(MetricRegistry.name("executor", "resource", "gpus"), new Gauge[Double] {
    override def getValue: Double = scheduler.getGpusUsed
  })
  // Number of GPUs vs max
  if (scheduler.getMaxGpus != 0) {
    metricRegistry.register(MetricRegistry.name("executor", "resource", "gpus_of_max"),
      new Gauge[Double] {
        // Note: See above div0 check before calling register()
        override def getValue: Double = scheduler.getGpusUsed / scheduler.getMaxGpus
      })
  }
  // Number of GPUs per task
  metricRegistry.register(MetricRegistry.name("executor", "resource", "mean_gpus_per_task"),
    new Gauge[Double] {
      override def getValue: Double = scheduler.getMeanGpusPerTask
    })

  // Number of tasks
  metricRegistry.register(MetricRegistry.name("executor", "count"), new Gauge[Int] {
    override def getValue: Int = scheduler.getTaskCount
  })
  // Number of tasks vs max
  if (scheduler.isExecutorLimitEnabled) {
    // executorLimit is assigned asynchronously, so it may start off with a zero value.
    metricRegistry.register(MetricRegistry.name("executor", "count_of_max"), new Gauge[Int] {
      override def getValue: Int = {
        if (scheduler.getExecutorLimit == 0) {
          0
        } else {
          scheduler.getTaskCount / scheduler.getExecutorLimit
        }
      }
    })
  }
  // Number of task failures
  metricRegistry.register(MetricRegistry.name("executor", "failures"), new Gauge[Int] {
    override def getValue: Int = scheduler.getTaskFailureCount
  })
  // Number of tracked agents regardless of whether we're currently present on them
  metricRegistry.register(MetricRegistry.name("executor", "known_agents"), new Gauge[Int] {
    override def getValue: Int = scheduler.getKnownAgentsCount
  })
  // Number of tracked agents with tasks on them
  metricRegistry.register(MetricRegistry.name("executor", "occupied_agents"), new Gauge[Int] {
    override def getValue: Int = scheduler.getOccupiedAgentsCount
  })
  // Number of blacklisted agents (too many failures)
  metricRegistry.register(MetricRegistry.name("executor", "blacklisted_agents"), new Gauge[Int] {
    override def getValue: Int = scheduler.getBlacklistedAgentCount
  })

  // MESOS EVENT PUSH METRICS:
  // These metrics measure events received from and sent to Mesos

  // Rate of offers received (total number of offers, not offer RPCs)
  private val offerCounter =
    metricRegistry.counter(MetricRegistry.name("executor", "mesos", "offer"))
  // Rate of all offers declined, sum of the following reasons for declines
  private val declineCounter =
    metricRegistry.counter(MetricRegistry.name("executor", "mesos", "decline"))
  // Offers declined for unmet requirements (with RejectOfferDurationForUnmetConstraints)
  private val declineUnmetCounter =
    metricRegistry.counter(MetricRegistry.name("executor", "mesos", "decline_unmet"))
  // Offers declined when the deployment is finished (with RejectOfferDurationForReachedMaxCores)
  private val declineFinishedCounter =
    metricRegistry.counter(MetricRegistry.name("executor", "mesos", "decline_finished"))
  // Offers declined when offers are being unused (no duration in the decline filter)
  private val declineUnusedCounter =
    metricRegistry.counter(MetricRegistry.name("executor", "mesos", "decline_unused"))
  // Rate of revive operations
  private val reviveCounter =
    metricRegistry.counter(MetricRegistry.name("executor", "mesos", "revive"))
  // Rate of launch operations
  private val launchCounter =
    metricRegistry.counter(MetricRegistry.name("executor", "mesos", "launch"))

  // Counters for Spark states on launched executors (LAUNCHING, RUNNING, ...)
  private val sparkStateCounters = TaskState.values
    .map(state => (state, metricRegistry.counter(
      MetricRegistry.name("executor", "spark_state", state.toString.toLowerCase))))
    .toMap
  private val sparkUnknownStateCounter =
    metricRegistry.counter(MetricRegistry.name("executor", "spark_state", "UNKNOWN"))
  // Counters for Mesos states on launched executors (TASK_RUNNING, TASK_LOST, ...),
  // more granular than sparkStateCounters
  private val mesosStateCounters = MesosTaskState.values
    .map(state => (state, metricRegistry.counter(
      MetricRegistry.name("executor", "mesos_state", state.name.toLowerCase))))
    .toMap
  private val mesosUnknownStateCounter =
    metricRegistry.counter(MetricRegistry.name("executor", "mesos_state", "UNKNOWN"))

  // TASK TIMER METRICS:
  // These metrics measure the duration to launch and run executors

  // Duration from driver start to the first task launching.
  private val startToFirstLaunched =
    metricRegistry.timer(MetricRegistry.name("executor", "start_to_first_launched"))
  // Duration from driver start to the first task running.
  private val startToFirstRunning =
    metricRegistry.timer(MetricRegistry.name("executor", "start_to_first_running"))

  // Duration from driver start to maxCores footprint being filled
  private val startToAllLaunched =
    metricRegistry.timer(MetricRegistry.name("executor", "start_to_all_launched"))

  // Duration between an executor launch and the executor entering a given spark state, e.g. RUNNING
  private val launchToSparkStateTimers = TaskState.values
    .map(state => (state, metricRegistry.timer(
      MetricRegistry.name("executor", "launch_to_spark_state", state.toString.toLowerCase))))
    .toMap
  private val launchToUnknownSparkStateTimer = metricRegistry.timer(
    MetricRegistry.name("executor", "launch_to_spark_state", "UNKNOWN"))

  // Time that the scheduler was initialized. This is the 'start time'.
  private val schedulerInitTime = new Date
  // Time that a given task was launched.
  private val taskLaunchTimeByTaskId = new HashMap[String, Date]

  // Whether we've had a task be launched or running yet (only record once)
  private val recordedFirstTaskLaunched = new AtomicBoolean(false)
  private val recordedFirstTaskRunning = new AtomicBoolean(false)
  // Whether we've had all tasks launched with cpu footprint reached (only record once)
  private val recordedAllTasksLaunched = new AtomicBoolean(false)

  def recordOffers(count: Int): Unit = offerCounter.inc(count)
  def recordDeclineUnmet(count: Int): Unit = {
    declineCounter.inc(count)
    declineUnmetCounter.inc(count)
  }
  def recordDeclineFinished: Unit = {
    declineCounter.inc
    declineFinishedCounter.inc
  }
  def recordDeclineUnused(count: Int): Unit = {
    declineCounter.inc(count)
    declineUnusedCounter.inc(count)
  }
  def recordRevive: Unit = reviveCounter.inc

  def recordTaskLaunch(taskId: String, footprintFilled: Boolean): Unit = {
    launchCounter.inc
    taskLaunchTimeByTaskId += (taskId -> new Date)

    if (!recordedFirstTaskLaunched.getAndSet(true)) {
      recordTimeSince(schedulerInitTime, startToFirstLaunched)
    }
    if (footprintFilled && !recordedAllTasksLaunched.getAndSet(true)) {
      recordTimeSince(schedulerInitTime, startToAllLaunched)
    }
  }

  def recordTaskStatus(taskId: String, mesosState: MesosTaskState, sparkState: TaskState.Value):
      Unit = {
    mesosStateCounters.get(mesosState) match {
      case Some(counter) => counter.inc
      case None => mesosUnknownStateCounter.inc
    }

    sparkStateCounters.get(sparkState) match {
      case Some(counter) => counter.inc
      case None => sparkUnknownStateCounter.inc
    }

    if (sparkState.equals(TaskState.RUNNING) && !recordedFirstTaskRunning.getAndSet(true)) {
      recordTimeSince(schedulerInitTime, startToFirstRunning)
    }

    taskLaunchTimeByTaskId.get(taskId) match {
      case Some(taskLaunchTime) =>
        launchToSparkStateTimers.get(sparkState) match {
          case Some(timer) => recordTimeSince(taskLaunchTime, timer)
          case None => recordTimeSince(taskLaunchTime, launchToUnknownSparkStateTimer)
        }

        if (TaskState.isFinished(sparkState)) {
          // Task finished: Remove from our tracking.
          taskLaunchTimeByTaskId -= taskId
        }
      case None =>
        // Unknown task: This can happen when Mesos tells us about a task that we're no longer
        // tracking. One case is when a very old Mesos agent with tasks reconnects to the master.
    }
  }

  private def recordTimeSince(date: Date, timer: Timer): Unit =
    timer.update(System.currentTimeMillis - date.getTime, TimeUnit.MILLISECONDS)
}
