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

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder

package object config {

  private[spark] class MesosSecretConfig private[config](taskType: String) {
    private[spark] val SECRET_NAMES =
      ConfigBuilder(s"spark.mesos.$taskType.secret.names")
        .doc("A comma-separated list of secret reference names. Consult the Mesos Secret " +
          "protobuf for more information.")
        .stringConf
        .toSequence
        .createOptional

    private[spark] val SECRET_VALUES =
      ConfigBuilder(s"spark.mesos.$taskType.secret.values")
        .doc("A comma-separated list of secret values.")
        .stringConf
        .toSequence
        .createOptional

    private[spark] val SECRET_ENVKEYS =
      ConfigBuilder(s"spark.mesos.$taskType.secret.envkeys")
        .doc("A comma-separated list of the environment variables to contain the secrets." +
          "The environment variable will be set on the driver.")
        .stringConf
        .toSequence
        .createOptional

    private[spark] val SECRET_FILENAMES =
      ConfigBuilder(s"spark.mesos.$taskType.secret.filenames")
        .doc("A comma-separated list of file paths secret will be written to.  Consult the Mesos " +
          "Secret protobuf for more information.")
        .stringConf
        .toSequence
        .createOptional
  }

  private[spark] val CREDENTIAL_PRINCIPAL =
    ConfigBuilder("spark.mesos.principal")
      .doc("Name of the Kerberos principal to authenticate Spark to Mesos.")
      .stringConf
      .createOptional

  private[spark] val CREDENTIAL_PRINCIPAL_FILE =
    ConfigBuilder("spark.mesos.principal.file")
      .doc("The path of file which contains the name of the Kerberos principal " +
        "to authenticate Spark to Mesos.")
      .stringConf
      .createOptional

  private[spark] val CREDENTIAL_SECRET =
    ConfigBuilder("spark.mesos.secret")
      .doc("The secret value to authenticate Spark to Mesos.")
      .stringConf
      .createOptional

  private[spark] val CREDENTIAL_SECRET_FILE =
    ConfigBuilder("spark.mesos.secret.file")
      .doc("The path of file which contains the secret value to authenticate Spark to Mesos.")
      .stringConf
      .createOptional

  /* Common app configuration. */

  private[spark] val SHUFFLE_CLEANER_INTERVAL_S =
    ConfigBuilder("spark.shuffle.cleaner.interval")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("30s")

  private[spark] val DISPATCHER_WEBUI_URL =
    ConfigBuilder("spark.mesos.dispatcher.webui.url")
      .doc("Set the Spark Mesos dispatcher webui_url for interacting with the " +
        "framework. If unset it will point to Spark's internal web UI.")
      .stringConf
      .createOptional

  private[spark] val HISTORY_SERVER_URL =
    ConfigBuilder("spark.mesos.dispatcher.historyServer.url")
      .doc("Set the URL of the history server. The dispatcher will then " +
        "link each driver to its entry in the history server.")
      .stringConf
      .createOptional

  private[spark] val DRIVER_LABELS =
    ConfigBuilder("spark.mesos.driver.labels")
      .doc("Mesos labels to add to the driver.  Labels are free-form key-value pairs. Key-value " +
        "pairs should be separated by a colon, and commas used to list more than one." +
        "Ex. key:value,key2:value2")
      .stringConf
      .createOptional

  private[spark] val DRIVER_WEBUI_URL =
    ConfigBuilder("spark.mesos.driver.webui.url")
      .stringConf
      .createOptional

  private[spark] val driverSecretConfig = new MesosSecretConfig("driver")

  private[spark] val executorSecretConfig = new MesosSecretConfig("executor")

  private[spark] val DRIVER_FAILOVER_TIMEOUT =
    ConfigBuilder("spark.mesos.driver.failoverTimeout")
      .doc("Amount of time in seconds that the master will wait to hear from the driver, " +
          "during a temporary disconnection, before tearing down all the executors.")
      .doubleConf
      .createWithDefault(0.0)

  private[spark] val NETWORK_NAME =
    ConfigBuilder("spark.mesos.network.name")
      .doc("Attach containers to the given named network. If this job is launched " +
        "in cluster mode, also launch the driver in the given named network.")
      .stringConf
      .createOptional

  private[spark] val NETWORK_LABELS =
    ConfigBuilder("spark.mesos.network.labels")
      .doc("Network labels to pass to CNI plugins.  This is a comma-separated list " +
        "of key-value pairs, where each key-value pair has the format key:value. " +
        "Example: key1:val1,key2:val2")
      .stringConf
      .createOptional

  private[spark] val DRIVER_CONSTRAINTS =
    ConfigBuilder("spark.mesos.driver.constraints")
      .doc("Attribute based constraints on mesos resource offers. Applied by the dispatcher " +
        "when launching drivers. Default is to accept all offers with sufficient resources.")
      .stringConf
      .createWithDefault("")

  private[spark] val DRIVER_FRAMEWORK_ID =
    ConfigBuilder("spark.mesos.driver.frameworkId")
      .stringConf
      .createOptional

  private[spark] val EXECUTOR_URI =
    ConfigBuilder("spark.executor.uri").stringConf.createOptional

  private[spark] val PROXY_BASE_URL =
    ConfigBuilder("spark.mesos.proxy.baseURL").stringConf.createOptional

  private[spark] val COARSE_MODE =
    ConfigBuilder("spark.mesos.coarse").booleanConf.createWithDefault(true)

  private[spark] val COARSE_SHUTDOWN_TIMEOUT =
    ConfigBuilder("spark.mesos.coarse.shutdownTimeout")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(_ >= 0, s"spark.mesos.coarse.shutdownTimeout must be >= 0")
      .createWithDefaultString("10s")

  private[spark] val MAX_DRIVERS =
    ConfigBuilder("spark.mesos.maxDrivers").intConf.createWithDefault(200)

  private[spark] val RETAINED_DRIVERS =
    ConfigBuilder("spark.mesos.retainedDrivers").intConf.createWithDefault(200)

  private[spark] val CLUSTER_RETRY_WAIT_MAX_SECONDS =
    ConfigBuilder("spark.mesos.cluster.retry.wait.max")
      .intConf
      .createWithDefault(60) // 1 minute

  private[spark] val ENABLE_FETCHER_CACHE =
    ConfigBuilder("spark.mesos.fetcherCache.enable")
      .booleanConf
      .createWithDefault(false)

  private[spark] val APP_JAR_LOCAL_RESOLUTION_MODE =
    ConfigBuilder("spark.mesos.appJar.local.resolution.mode")
      .stringConf
      .checkValues(Set("host", "container"))
      .createWithDefault("host")

  private[spark] val REJECT_OFFER_DURATION =
    ConfigBuilder("spark.mesos.rejectOfferDuration")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("120s")

  private[spark] val REJECT_OFFER_DURATION_FOR_UNMET_CONSTRAINTS =
    ConfigBuilder("spark.mesos.rejectOfferDurationForUnmetConstraints")
      .timeConf(TimeUnit.SECONDS)
      .createOptional

  private[spark] val REJECT_OFFER_DURATION_FOR_REACHED_MAX_CORES =
    ConfigBuilder("spark.mesos.rejectOfferDurationForReachedMaxCores")
      .timeConf(TimeUnit.SECONDS)
      .createOptional

  private[spark] val URIS_TO_DOWNLOAD =
    ConfigBuilder("spark.mesos.uris")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  private[spark] val EXECUTOR_HOME =
    ConfigBuilder("spark.mesos.executor.home")
      .stringConf
      .createOptional

  private[spark] val EXECUTOR_CORES =
    ConfigBuilder("spark.mesos.mesosExecutor.cores")
      .doubleConf
      .createWithDefault(1)

  private[spark] val EXTRA_CORES_PER_EXECUTOR =
    ConfigBuilder("spark.mesos.extra.cores")
      .intConf
      .createWithDefault(0)

  private[spark] val EXECUTOR_MEMORY_OVERHEAD =
    ConfigBuilder("spark.mesos.executor.memoryOverhead")
      .intConf
      .createOptional

  private[spark] val EXECUTOR_DOCKER_IMAGE =
    ConfigBuilder("spark.mesos.executor.docker.image")
      .stringConf
      .createOptional

  private[spark] val EXECUTOR_DOCKER_FORCE_PULL_IMAGE =
    ConfigBuilder("spark.mesos.executor.docker.forcePullImage")
      .booleanConf
      .createOptional

  private[spark] val EXECUTOR_DOCKER_PORT_MAPS =
    ConfigBuilder("spark.mesos.executor.docker.portmaps")
      .stringConf
      .toSequence
      .createOptional

  private[spark] val EXECUTOR_DOCKER_PARAMETERS =
    ConfigBuilder("spark.mesos.executor.docker.parameters")
      .stringConf
      .toSequence
      .createOptional

  private[spark] val EXECUTOR_DOCKER_VOLUMES =
    ConfigBuilder("spark.mesos.executor.docker.volumes")
      .stringConf
      .toSequence
      .createOptional

  private[spark] val MAX_GPUS =
    ConfigBuilder("spark.mesos.gpus.max")
      .intConf
      .createWithDefault(0)

  private[spark] val TASK_LABELS =
    ConfigBuilder("spark.mesos.task.labels")
      .stringConf
      .createWithDefault("")

  private[spark] val CONSTRAINTS =
    ConfigBuilder("spark.mesos.constraints")
      .stringConf
      .createWithDefault("")

  private[spark] val CONTAINERIZER =
    ConfigBuilder("spark.mesos.containerizer")
      .stringConf
      .checkValues(Set("docker", "mesos"))
      .createWithDefault("docker")

  private[spark] val ROLE =
    ConfigBuilder("spark.mesos.role")
      .stringConf
      .createOptional

  private[spark] val DRIVER_ENV_PREFIX = "spark.mesos.driverEnv."
  private[spark] val DISPATCHER_DRIVER_DEFAULT_PREFIX = "spark.mesos.dispatcher.driverDefault."
}
