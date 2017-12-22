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
package org.apache.spark.deploy.k8s

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigBuilder

private[spark] object Config extends Logging {

  val KUBERNETES_NAMESPACE =
    ConfigBuilder("spark.kubernetes.namespace")
      .doc("The namespace that will be used for running the driver and executor pods.")
      .stringConf
      .createWithDefault("default")

  val DRIVER_CONTAINER_IMAGE =
    ConfigBuilder("spark.kubernetes.driver.container.image")
      .doc("Container image to use for the driver.")
      .stringConf
      .createOptional

  val EXECUTOR_CONTAINER_IMAGE =
    ConfigBuilder("spark.kubernetes.executor.container.image")
      .doc("Container image to use for the executors.")
      .stringConf
      .createOptional

  val CONTAINER_IMAGE_PULL_POLICY =
    ConfigBuilder("spark.kubernetes.container.image.pullPolicy")
      .doc("Kubernetes image pull policy. Valid values are Always, Never, and IfNotPresent.")
      .stringConf
      .checkValues(Set("Always", "Never", "IfNotPresent"))
      .createWithDefault("IfNotPresent")

  val KUBERNETES_AUTH_DRIVER_CONF_PREFIX =
      "spark.kubernetes.authenticate.driver"
  val KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX =
      "spark.kubernetes.authenticate.driver.mounted"
  val OAUTH_TOKEN_CONF_SUFFIX = "oauthToken"
  val OAUTH_TOKEN_FILE_CONF_SUFFIX = "oauthTokenFile"
  val CLIENT_KEY_FILE_CONF_SUFFIX = "clientKeyFile"
  val CLIENT_CERT_FILE_CONF_SUFFIX = "clientCertFile"
  val CA_CERT_FILE_CONF_SUFFIX = "caCertFile"

  val KUBERNETES_SERVICE_ACCOUNT_NAME =
    ConfigBuilder(s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.serviceAccountName")
      .doc("Service account that is used when running the driver pod. The driver pod uses " +
        "this service account when requesting executor pods from the API server. If specific " +
        "credentials are given for the driver pod to use, the driver will favor " +
        "using those credentials instead.")
      .stringConf
      .createOptional

  val KUBERNETES_DRIVER_LIMIT_CORES =
    ConfigBuilder("spark.kubernetes.driver.limit.cores")
      .doc("Specify the hard cpu limit for the driver pod")
      .stringConf
      .createOptional

  val KUBERNETES_EXECUTOR_LIMIT_CORES =
    ConfigBuilder("spark.kubernetes.executor.limit.cores")
      .doc("Specify the hard cpu limit for each executor pod")
      .stringConf
      .createOptional

  val KUBERNETES_DRIVER_POD_NAME =
    ConfigBuilder("spark.kubernetes.driver.pod.name")
      .doc("Name of the driver pod.")
      .stringConf
      .createOptional

  val KUBERNETES_EXECUTOR_POD_NAME_PREFIX =
    ConfigBuilder("spark.kubernetes.executor.podNamePrefix")
      .doc("Prefix to use in front of the executor pod names.")
      .internal()
      .stringConf
      .createWithDefault("spark")

  val KUBERNETES_ALLOCATION_BATCH_SIZE =
    ConfigBuilder("spark.kubernetes.allocation.batch.size")
      .doc("Number of pods to launch at once in each round of executor allocation.")
      .intConf
      .checkValue(value => value > 0, "Allocation batch size should be a positive integer")
      .createWithDefault(5)

  val KUBERNETES_ALLOCATION_BATCH_DELAY =
    ConfigBuilder("spark.kubernetes.allocation.batch.delay")
      .doc("Time to wait between each round of executor allocation.")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(value => value > 0, "Allocation batch delay must be a positive time value.")
      .createWithDefaultString("1s")

  val KUBERNETES_EXECUTOR_LOST_REASON_CHECK_MAX_ATTEMPTS =
    ConfigBuilder("spark.kubernetes.executor.lostCheck.maxAttempts")
      .doc("Maximum number of attempts allowed for checking the reason of an executor loss " +
        "before it is assumed that the executor failed.")
      .intConf
      .checkValue(value => value > 0, "Maximum attempts of checks of executor lost reason " +
        "must be a positive integer")
      .createWithDefault(10)

  val WAIT_FOR_APP_COMPLETION =
    ConfigBuilder("spark.kubernetes.submission.waitAppCompletion")
      .doc("In cluster mode, whether to wait for the application to finish before exiting the " +
        "launcher process.")
      .booleanConf
      .createWithDefault(true)

  val REPORT_INTERVAL =
    ConfigBuilder("spark.kubernetes.report.interval")
      .doc("Interval between reports of the current app status in cluster mode.")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(interval => interval > 0, s"Logging interval must be a positive time value.")
      .createWithDefaultString("1s")

  val JARS_DOWNLOAD_LOCATION =
    ConfigBuilder("spark.kubernetes.mountDependencies.jarsDownloadDir")
      .doc("Location to download jars to in the driver and executors. When using " +
        "spark-submit, this directory must be empty and will be mounted as an empty directory " +
        "volume on the driver and executor pod.")
      .stringConf
      .createWithDefault("/var/spark-data/spark-jars")

  val FILES_DOWNLOAD_LOCATION =
    ConfigBuilder("spark.kubernetes.mountDependencies.filesDownloadDir")
      .doc("Location to download files to in the driver and executors. When using " +
        "spark-submit, this directory must be empty and will be mounted as an empty directory " +
        "volume on the driver and executor pods.")
      .stringConf
      .createWithDefault("/var/spark-data/spark-files")

  val INIT_CONTAINER_IMAGE =
    ConfigBuilder("spark.kubernetes.initContainer.image")
      .doc("Image for the driver and executor's init-container for downloading dependencies.")
      .stringConf
      .createOptional

  val INIT_CONTAINER_MOUNT_TIMEOUT =
    ConfigBuilder("spark.kubernetes.mountDependencies.timeout")
      .doc("Timeout before aborting the attempt to download and unpack dependencies from remote " +
        "locations into the driver and executor pods.")
      .timeConf(TimeUnit.MINUTES)
      .createWithDefault(5)

  val INIT_CONTAINER_MAX_THREAD_POOL_SIZE =
    ConfigBuilder("spark.kubernetes.mountDependencies.maxSimultaneousDownloads")
      .doc("Maximum number of remote dependencies to download simultaneously in a driver or " +
        "executor pod.")
      .intConf
      .createWithDefault(5)

  val INIT_CONTAINER_REMOTE_JARS =
    ConfigBuilder("spark.kubernetes.initContainer.remoteJars")
      .doc("Comma-separated list of jar URIs to download in the init-container. This is " +
        "calculated from spark.jars.")
      .internal()
      .stringConf
      .createOptional

  val INIT_CONTAINER_REMOTE_FILES =
    ConfigBuilder("spark.kubernetes.initContainer.remoteFiles")
      .doc("Comma-separated list of file URIs to download in the init-container. This is " +
        "calculated from spark.files.")
      .internal()
      .stringConf
      .createOptional

  val INIT_CONTAINER_CONFIG_MAP_NAME =
    ConfigBuilder("spark.kubernetes.initContainer.configMapName")
      .doc("Name of the config map to use in the init-container that retrieves submitted files " +
        "for the executor.")
      .internal()
      .stringConf
      .createOptional

  val INIT_CONTAINER_CONFIG_MAP_KEY_CONF =
    ConfigBuilder("spark.kubernetes.initContainer.configMapKey")
      .doc("Key for the entry in the init container config map for submitted files that " +
        "corresponds to the properties for this init-container.")
      .internal()
      .stringConf
      .createOptional

  val KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX =
    "spark.kubernetes.authenticate.submission"

  val KUBERNETES_NODE_SELECTOR_PREFIX = "spark.kubernetes.node.selector."

  val KUBERNETES_DRIVER_LABEL_PREFIX = "spark.kubernetes.driver.label."
  val KUBERNETES_DRIVER_ANNOTATION_PREFIX = "spark.kubernetes.driver.annotation."
  val KUBERNETES_DRIVER_SECRETS_PREFIX = "spark.kubernetes.driver.secrets."

  val KUBERNETES_EXECUTOR_LABEL_PREFIX = "spark.kubernetes.executor.label."
  val KUBERNETES_EXECUTOR_ANNOTATION_PREFIX = "spark.kubernetes.executor.annotation."
  val KUBERNETES_EXECUTOR_SECRETS_PREFIX = "spark.kubernetes.executor.secrets."

  val KUBERNETES_DRIVER_ENV_KEY = "spark.kubernetes.driverEnv."
}
