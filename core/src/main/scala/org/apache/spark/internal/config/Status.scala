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

package org.apache.spark.internal.config

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf.buildConf

private[spark] object Status {

  val ASYNC_TRACKING_ENABLED = buildConf("spark.appStateStore.asyncTracking.enable")
    .booleanConf
    .createWithDefault(true)

  val LIVE_ENTITY_UPDATE_PERIOD = buildConf("spark.ui.liveUpdate.period")
    .timeConf(TimeUnit.NANOSECONDS)
    .createWithDefaultString("100ms")

  val MAX_RETAINED_JOBS = buildConf("spark.ui.retainedJobs")
    .intConf
    .createWithDefault(1000)

  val MAX_RETAINED_STAGES = buildConf("spark.ui.retainedStages")
    .intConf
    .createWithDefault(1000)

  val MAX_RETAINED_TASKS_PER_STAGE = buildConf("spark.ui.retainedTasks")
    .intConf
    .createWithDefault(100000)

  val MAX_RETAINED_DEAD_EXECUTORS = buildConf("spark.ui.retainedDeadExecutors")
    .intConf
    .createWithDefault(100)

  val MAX_RETAINED_ROOT_NODES = buildConf("spark.ui.dagGraph.retainedRootRDDs")
    .intConf
    .createWithDefault(Int.MaxValue)

  val APP_STATUS_METRICS_ENABLED =
    buildConf("spark.app.status.metrics.enabled")
      .doc("Whether Dropwizard/Codahale metrics " +
        "will be reported for the status of the running spark app.")
      .booleanConf
      .createWithDefault(false)
}
