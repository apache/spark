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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.hashicorp.nomad.apimodel._
import org.apache.commons.lang3.StringUtils

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.scheduler.cluster.nomad.SparkNomadJob.applyConf

/**
 * Defines configuration for a type of Nomad task group that is created by Spark.
 */
private[spark] abstract class SparkNomadTaskGroupType(
    val role: String,
    val tasks: SparkNomadTaskType*
) {

  val RETRY_ATTEMPTS =
    ConfigBuilder(s"spark.nomad.$role.retryAttempts")
      .doc(s"The number of times Nomad should retry $role task groups if they fail")
      .intConf
      .createWithDefault(5)

  val RETRY_DELAY_NANOS =
    ConfigBuilder(s"spark.nomad.$role.retryDelay")
      .doc(s"How long Nomad should wait before retrying $role task groups if they fail")
      .timeConf(TimeUnit.NANOSECONDS)
      .createWithDefaultString("15s")

  val RETRY_INTERVAL_NANOS =
    ConfigBuilder(s"spark.nomad.$role.retryInterval")
      .doc(s"Nomad's retry interval for $role task groups")
      .timeConf(TimeUnit.NANOSECONDS)
      .createWithDefaultString("1d")

  protected def configureCommonSettings(
      jobConf: SparkNomadJob.CommonConf,
      conf: SparkConf,
      group: TaskGroup
  ): Unit = {

    if (StringUtils.isEmpty(group.getName)) {
      group.setName(role)
    }

    group.setCount(1)

    val policy = Option(group.getRestartPolicy).getOrElse {
      val p = new RestartPolicy
      group.setRestartPolicy(p)
      p
    }
    if (StringUtils.isEmpty(policy.getMode)) {
      policy.setMode("fail")
    }
    applyConf(conf, RETRY_ATTEMPTS, policy.getAttempts)(policy.setAttempts(_))
    applyConf(conf, RETRY_DELAY_NANOS, policy.getDelay)(policy.setDelay(_))
    applyConf(conf, RETRY_INTERVAL_NANOS, policy.getInterval)(policy.setInterval(_))
  }

  protected def find(taskGroup: TaskGroup, taskType: SparkNomadTaskType): Option[Task] = {
    taskGroup.getTasks match {
      case null => None
      case tasks => tasks.asScala.find(taskType.isTypeOf)
    }
  }

  protected def findOrAdd(group: TaskGroup, taskType: SparkNomadTaskType): Task =
    find(group, taskType).getOrElse {
      val task = taskType.newBlankTask()
      group.addTasks(task)
      task
    }

}
