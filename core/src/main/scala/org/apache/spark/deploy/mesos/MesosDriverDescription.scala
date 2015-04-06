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

package org.apache.spark.deploy.mesos

import java.util.Date

import org.apache.spark.deploy.Command

/**
 * Describes a Spark driver that is submitted from the
 * [[org.apache.spark.deploy.rest.mesos.MesosRestServer]], to be launched by
 * [[org.apache.spark.scheduler.cluster.mesos.MesosClusterScheduler]].
 * @param jarUrl URL to the application jar
 * @param mem Amount of memory for the driver
 * @param cores Amount of cores for the driver
 * @param supervise Supervise the driver for long running app
 * @param command The command to launch the driver.
 * @param schedulerProperties Extra properties to pass the Mesos scheduler
 */
private[spark] case class MesosDriverDescription(
    val name: String,
    val jarUrl: String,
    val mem: Int,
    val cores: Double,
    val supervise: Boolean,
    val command: Command,
    val schedulerProperties: Map[String, String])
  extends Serializable {

  var submissionId: Option[String] = None
  var submissionDate: Option[Date] = None

  override def toString: String = s"MesosDriverDescription (${command.mainClass})"
}
