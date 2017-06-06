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

package org.apache.spark.scheduler.cluster.nomad

import com.hashicorp.nomad.apimodel.TaskGroup

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.SHUFFLE_SERVICE_ENABLED
import org.apache.spark.scheduler.cluster.nomad.SparkNomadJob.CommonConf

private[spark] object ExecutorTaskGroup
  extends SparkNomadTaskGroupType("executor", ExecutorTask, ShuffleServiceTask) {

  def configure(
      jobConf: SparkNomadJob.CommonConf,
      conf: SparkConf,
      group: TaskGroup,
      reconfiguring: Boolean
  ): Unit = {
    configureCommonSettings(jobConf, conf, group)

    group.setCount(0)

    val shuffleServicePortPlaceholder =
      if (conf.get(SHUFFLE_SERVICE_ENABLED)) {
        val task = findOrAdd(group, ShuffleServiceTask)
        Some(ShuffleServiceTask.configure(jobConf, conf, task, reconfiguring))
      } else {
        find(group, ShuffleServiceTask).foreach(group.getTasks.remove)
        None
      }

    ExecutorTask.configure(
      jobConf, conf, findOrAdd(group, ExecutorTask), shuffleServicePortPlaceholder, reconfiguring)
  }

  def initialize(
      conf: SparkConf,
      group: TaskGroup,
      driverUrl: String,
      initialExecutors: Int
  ): Unit = {
    val task = find(group, ExecutorTask).get
    configure(CommonConf(conf), conf, group, reconfiguring = true)
    ExecutorTask.addDriverUrlArguments(task, driverUrl)

    group.setCount(initialExecutors)
  }

}
