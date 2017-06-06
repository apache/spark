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

package org.apache.spark.deploy.nomad

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.nomad.NomadClusterModeLauncher._
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.cluster.nomad._
import org.apache.spark.util.Utils

/**
 * Configuration for submitting an application to nomad in cluster mode.
 *
 * In order to fail fast in the face of configuration errors,
 * we extract all the configuration as soon as possible, including backend config.
 * This allows us fail before we interact with Nomad at all, instead of at some later point.
 */
private[spark] case class NomadClusterModeConf(
    backend: NomadClusterManagerConf,
    expectImmediateScheduling: Boolean,
    monitorUntil: Option[Milestone]
)

private[spark] object NomadClusterModeConf {

  val EXPECT_IMMEDIATE_SCHEDULING =
    ConfigBuilder("spark.nomad.cluster.expectImmediateScheduling")
      .doc("When true, spark-submit will fail if Nomad isn't able to schedule the job to run " +
        "right away")
      .booleanConf
      .createWithDefault(false)

  val MONITOR_UNTIL =
    ConfigBuilder("spark.nomad.cluster.monitorUntil")
      .doc("Specifies how long spark-submit should monitor a Spark application in cluster mode. " +
        "`submitted` (the default) causes spark-submit to return as soon as the application has " +
        "been submitted to the Nomad cluster. `scheduled` causes spark-submit to return once " +
        "the Nomad job has been scheduled. `complete` causes spark-submit to tail the output " +
        "from the driver process and return when the job has completed.")
      .stringConf
      .createOptional

  def apply(conf: SparkConf, command: ApplicationRunCommand): NomadClusterModeConf = {

    val backendConf = NomadClusterManagerConf(conf, Some(command))

    // Fail fast if dynamic execution is enabled but the external shuffle service isn't
    // (otherwise we would create a Nomad job to run the driver, and it would fail to start)
    if (Utils.isDynamicAllocationEnabled(conf) && !conf.get(SHUFFLE_SERVICE_ENABLED)) {
      throw new SparkException("Dynamic allocation of executors requires the external " +
        "shuffle service. You may enable this through spark.shuffle.service.enabled.")
    }

    val expectImmediateScheduling = conf.get(EXPECT_IMMEDIATE_SCHEDULING)

    NomadClusterModeConf(
      backend = backendConf,
      expectImmediateScheduling = expectImmediateScheduling,
      monitorUntil = conf.get(MONITOR_UNTIL).map(_.toLowerCase() match {
        case "submitted" if expectImmediateScheduling =>
          throw new IllegalArgumentException(
            s"$EXPECT_IMMEDIATE_SCHEDULING is true but $MONITOR_UNTIL is `submitted`")
        case "submitted" => Submitted
        case "scheduled" => Scheduled
        case "complete" => Complete
        case value =>
          throw new IllegalArgumentException(
            """spark.nomad.traceUntil must be "submitted", "scheduled" or "complete""""
          )
      })

    )
  }

}
