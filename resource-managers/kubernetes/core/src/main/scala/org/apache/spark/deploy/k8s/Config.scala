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
import org.apache.spark.network.util.ByteUnit

private[spark] object Config extends Logging {

  val KUBERNETES_NAMESPACE =
    ConfigBuilder("spark.kubernetes.namespace")
      .doc("The namespace that will be used for running the driver and executor pods. When using " +
        "spark-submit in cluster mode, this can also be passed to spark-submit via the " +
        "--kubernetes-namespace command line argument.")
      .stringConf
      .createWithDefault("default")

  val DRIVER_DOCKER_IMAGE =
    ConfigBuilder("spark.kubernetes.driver.docker.image")
      .doc("Docker image to use for the driver. Specify this using the standard Docker tag format.")
      .stringConf
      .createOptional

  val EXECUTOR_DOCKER_IMAGE =
    ConfigBuilder("spark.kubernetes.executor.docker.image")
      .doc("Docker image to use for the executors. Specify this using the standard Docker tag " +
        "format.")
      .stringConf
      .createOptional

  val DOCKER_IMAGE_PULL_POLICY =
    ConfigBuilder("spark.kubernetes.docker.image.pullPolicy")
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
      .doc("Specify the hard cpu limit for a single executor pod")
      .stringConf
      .createOptional

  val KUBERNETES_DRIVER_MEMORY_OVERHEAD =
    ConfigBuilder("spark.kubernetes.driver.memoryOverhead")
      .doc("The amount of off-heap memory (in megabytes) to be allocated for the driver and the " +
        "driver submission server. This is memory that accounts for things like VM overheads, " +
        "interned strings, other native overheads, etc. This tends to grow with the driver's " +
        "memory size (typically 6-10%).")
      .bytesConf(ByteUnit.MiB)
      .createOptional

  // Note that while we set a default for this when we start up the
  // scheduler, the specific default value is dynamically determined
  // based on the executor memory.
  val KUBERNETES_EXECUTOR_MEMORY_OVERHEAD =
    ConfigBuilder("spark.kubernetes.executor.memoryOverhead")
      .doc("The amount of off-heap memory (in megabytes) to be allocated per executor. This " +
        "is memory that accounts for things like VM overheads, interned strings, other native " +
        "overheads, etc. This tends to grow with the executor size. (typically 6-10%).")
      .bytesConf(ByteUnit.MiB)
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
      .doc("Number of seconds to wait between each round of executor allocation.")
      .longConf
      .checkValue(value => value > 0, "Allocation batch delay should be a positive integer")
      .createWithDefault(1)

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
      .createWithDefaultString("1s")

  private[spark] val JARS_DOWNLOAD_LOCATION =
    ConfigBuilder("spark.kubernetes.mountDependencies.jarsDownloadDir")
      .doc("Location to download jars to in the driver and executors. When using" +
        " spark-submit, this directory must be empty and will be mounted as an empty directory" +
        " volume on the driver and executor pod.")
      .stringConf
      .createWithDefault("/var/spark-data/spark-jars")

  private[spark] val FILES_DOWNLOAD_LOCATION =
    ConfigBuilder("spark.kubernetes.mountDependencies.filesDownloadDir")
      .doc("Location to download files to in the driver and executors. When using" +
        " spark-submit, this directory must be empty and will be mounted as an empty directory" +
        " volume on the driver and executor pods.")
      .stringConf
      .createWithDefault("/var/spark-data/spark-files")

  val KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX =
    "spark.kubernetes.authenticate.submission"

  val KUBERNETES_NODE_SELECTOR_PREFIX = "spark.kubernetes.node.selector."

  val KUBERNETES_DRIVER_LABEL_PREFIX = "spark.kubernetes.driver.label."
  val KUBERNETES_DRIVER_ANNOTATION_PREFIX = "spark.kubernetes.driver.annotation."

  val KUBERNETES_EXECUTOR_LABEL_PREFIX = "spark.kubernetes.executor.label."
  val KUBERNETES_EXECUTOR_ANNOTATION_PREFIX = "spark.kubernetes.executor.annotation."

  val KUBERNETES_DRIVER_ENV_KEY = "spark.kubernetes.driverEnv."

  def getK8sMasterUrl(rawMasterString: String): String = {
    require(rawMasterString.startsWith("k8s://"),
      "Master URL should start with k8s:// in Kubernetes mode.")
    val masterWithoutK8sPrefix = rawMasterString.substring("k8s://".length)
    if (masterWithoutK8sPrefix.startsWith("http://")
      || masterWithoutK8sPrefix.startsWith("https://")) {
      masterWithoutK8sPrefix
    } else {
      val resolvedURL = s"https://$masterWithoutK8sPrefix"
      logInfo("No scheme specified for kubernetes master URL, so defaulting to https. Resolved " +
        s"URL is $resolvedURL")
      resolvedURL
    }
  }
}
