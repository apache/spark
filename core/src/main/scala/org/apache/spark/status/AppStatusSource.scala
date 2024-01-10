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
package org.apache.spark.status

import java.util.concurrent.atomic.AtomicLong

import AppStatusSource.getCounter
import com.codahale.metrics.{Counter, Gauge, MetricRegistry}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Status.METRICS_APP_STATUS_SOURCE_ENABLED
import org.apache.spark.metrics.source.Source

private [spark] class JobDuration(val value: AtomicLong) extends Gauge[Long] {
  override def getValue: Long = value.get()
}

private[spark] class AppStatusSource extends Source {

  override implicit val metricRegistry: MetricRegistry = new MetricRegistry()

  override val sourceName = "appStatus"

  val jobDuration = new JobDuration(new AtomicLong(0L))

  // Duration of each job in milliseconds
  val JOB_DURATION = metricRegistry
    .register(MetricRegistry.name("jobDuration"), jobDuration)

  val FAILED_STAGES = getCounter("stages", "failedStages")

  val SKIPPED_STAGES = getCounter("stages", "skippedStages")

  val COMPLETED_STAGES = getCounter("stages", "completedStages")

  val SUCCEEDED_JOBS = getCounter("jobs", "succeededJobs")

  val FAILED_JOBS = getCounter("jobs", "failedJobs")

  val COMPLETED_TASKS = getCounter("tasks", "completedTasks")

  val FAILED_TASKS = getCounter("tasks", "failedTasks")

  val KILLED_TASKS = getCounter("tasks", "killedTasks")

  val SKIPPED_TASKS = getCounter("tasks", "skippedTasks")

  // This is the count of how many executors have been blacklisted at the application level,
  // does not include stage level blacklisting.
  // this is private but user visible from metrics so just deprecate
  @deprecated("use excludedExecutors instead", "3.1.0")
  val BLACKLISTED_EXECUTORS = getCounter("tasks", "blackListedExecutors")

  // This is the count of how many executors have been unblacklisted at the application level,
  // does not include stage level unblacklisting.
  @deprecated("use unexcludedExecutors instead", "3.1.0")
  val UNBLACKLISTED_EXECUTORS = getCounter("tasks", "unblackListedExecutors")

  // This is the count of how many executors have been excluded at the application level,
  // does not include stage level exclusion.
  val EXCLUDED_EXECUTORS = getCounter("tasks", "excludedExecutors")

  // This is the count of how many executors have been unexcluded at the application level,
  // does not include stage level unexclusion.
  val UNEXCLUDED_EXECUTORS = getCounter("tasks", "unexcludedExecutors")

}

private[spark] object AppStatusSource {

  def getCounter(prefix: String, name: String)(implicit metricRegistry: MetricRegistry): Counter = {
    metricRegistry.counter(MetricRegistry.name(prefix, name))
  }

  def createSource(conf: SparkConf): Option[AppStatusSource] = {
    Option(conf.get(METRICS_APP_STATUS_SOURCE_ENABLED))
      .filter(identity)
      .map { _ => new AppStatusSource() }
  }
}
