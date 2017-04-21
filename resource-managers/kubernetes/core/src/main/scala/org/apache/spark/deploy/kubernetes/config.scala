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
package org.apache.spark.deploy.kubernetes

import java.util.concurrent.TimeUnit

import org.apache.spark.{SPARK_VERSION => sparkVersion}
import org.apache.spark.deploy.kubernetes.submit.v1.NodePortUrisDriverServiceManager
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

package object config {

  private[spark] val KUBERNETES_NAMESPACE =
    ConfigBuilder("spark.kubernetes.namespace")
      .doc("The namespace that will be used for running the driver and executor pods. When using" +
        " spark-submit in cluster mode, this can also be passed to spark-submit via the" +
        " --kubernetes-namespace command line argument.")
      .stringConf
      .createWithDefault("default")

  private[spark] val DRIVER_DOCKER_IMAGE =
    ConfigBuilder("spark.kubernetes.driver.docker.image")
      .doc("Docker image to use for the driver. Specify this using the standard Docker tag format.")
      .stringConf
      .createWithDefault(s"spark-driver:$sparkVersion")

  private[spark] val EXECUTOR_DOCKER_IMAGE =
    ConfigBuilder("spark.kubernetes.executor.docker.image")
      .doc("Docker image to use for the executors. Specify this using the standard Docker tag" +
        " format.")
      .stringConf
      .createWithDefault(s"spark-executor:$sparkVersion")

  private val APISERVER_SUBMIT_CONF_PREFIX = "spark.kubernetes.authenticate.submission"
  private val APISERVER_DRIVER_CONF_PREFIX = "spark.kubernetes.authenticate.driver"

  private[spark] val KUBERNETES_SUBMIT_CA_CERT_FILE =
    ConfigBuilder(s"$APISERVER_SUBMIT_CONF_PREFIX.caCertFile")
      .doc("Path to the CA cert file for connecting to Kubernetes over SSL when creating" +
        " Kubernetes resources for the driver. This file should be located on the submitting" +
        " machine's disk.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_SUBMIT_CLIENT_KEY_FILE =
    ConfigBuilder(s"$APISERVER_SUBMIT_CONF_PREFIX.clientKeyFile")
      .doc("Path to the client key file for authenticating against the Kubernetes API server" +
        " when initially creating Kubernetes resources for the driver. This file should be" +
        " located on the submitting machine's disk.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_SUBMIT_CLIENT_CERT_FILE =
    ConfigBuilder(s"$APISERVER_SUBMIT_CONF_PREFIX.clientCertFile")
      .doc("Path to the client cert file for authenticating against the Kubernetes API server" +
        " when initially creating Kubernetes resources for the driver. This file should be" +
        " located on the submitting machine's disk.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_SUBMIT_OAUTH_TOKEN =
    ConfigBuilder(s"$APISERVER_SUBMIT_CONF_PREFIX.oauthToken")
      .doc("OAuth token to use when authenticating against the against the Kubernetes API server" +
        " when initially creating Kubernetes resources for the driver. Note that unlike the other" +
        " authentication options, this should be the exact string value of the token to use for" +
        " the authentication.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_CA_CERT_FILE =
    ConfigBuilder(s"$APISERVER_DRIVER_CONF_PREFIX.caCertFile")
      .doc("Path to the CA cert file for connecting to Kubernetes over TLS from the driver pod" +
        " when requesting executors. This file should be located on the submitting machine's disk" +
        " and will be uploaded to the driver pod.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_CLIENT_KEY_FILE =
    ConfigBuilder(s"$APISERVER_DRIVER_CONF_PREFIX.clientKeyFile")
      .doc("Path to the client key file for authenticating against the Kubernetes API server from" +
        " the driver pod when requesting executors. This file should be located on the submitting" +
        " machine's disk, and will be uploaded to the driver pod.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_CLIENT_CERT_FILE =
    ConfigBuilder(s"$APISERVER_DRIVER_CONF_PREFIX.clientCertFile")
      .doc("Path to the client cert file for authenticating against the Kubernetes API server" +
        " from the driver pod when requesting executors. This file should be located on the" +
        " submitting machine's disk, and will be uploaded to the driver pod.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_OAUTH_TOKEN =
    ConfigBuilder(s"$APISERVER_DRIVER_CONF_PREFIX.oauthToken")
      .doc("OAuth token to use when authenticating against the Kubernetes API server from the" +
        " driver pod when requesting executors. Note that unlike the other authentication options" +
        " this should be the exact string value of the token to use for the authentication. This" +
        " token value is mounted as a secret on the driver pod.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_MOUNTED_CA_CERT_FILE =
    ConfigBuilder(s"$APISERVER_DRIVER_CONF_PREFIX.mounted.caCertFile")
      .doc("Path on the driver pod's disk containing the CA cert file to use when authenticating" +
        " against Kubernetes.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_MOUNTED_CLIENT_KEY_FILE =
    ConfigBuilder(s"$APISERVER_DRIVER_CONF_PREFIX.mounted.clientKeyFile")
      .doc("Path on the driver pod's disk containing the client key file to use when" +
        " authenticating against Kubernetes.")
      .internal()
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_MOUNTED_CLIENT_CERT_FILE =
    ConfigBuilder(s"$APISERVER_DRIVER_CONF_PREFIX.mounted.clientCertFile")
      .doc("Path on the driver pod's disk containing the client cert file to use when" +
        " authenticating against Kubernetes.")
      .internal()
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_MOUNTED_OAUTH_TOKEN =
    ConfigBuilder(s"$APISERVER_DRIVER_CONF_PREFIX.mounted.oauthTokenFile")
      .doc("Path on the driver pod's disk containing the OAuth token file to use when" +
        " authenticating against Kubernetes.")
      .internal()
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_SERVICE_ACCOUNT_NAME =
    ConfigBuilder(s"$APISERVER_DRIVER_CONF_PREFIX.serviceAccountName")
      .doc("Service account that is used when running the driver pod. The driver pod uses" +
        " this service account when requesting executor pods from the API server. If specific" +
        " credentials are given for the driver pod to use, the driver will favor" +
        " using those credentials instead.")
      .stringConf
      .createOptional

  // Note that while we set a default for this when we start up the
  // scheduler, the specific default value is dynamically determined
  // based on the executor memory.
  private[spark] val KUBERNETES_EXECUTOR_MEMORY_OVERHEAD =
    ConfigBuilder("spark.kubernetes.executor.memoryOverhead")
      .doc("The amount of off-heap memory (in megabytes) to be allocated per executor. This" +
        " is memory that accounts for things like VM overheads, interned strings, other native" +
        " overheads, etc. This tends to grow with the executor size. (typically 6-10%).")
      .bytesConf(ByteUnit.MiB)
      .createOptional

  private[spark] val KUBERNETES_DRIVER_MEMORY_OVERHEAD =
    ConfigBuilder("spark.kubernetes.driver.memoryOverhead")
      .doc("The amount of off-heap memory (in megabytes) to be allocated for the driver and the" +
        " driver submission server. This is memory that accounts for things like VM overheads," +
        " interned strings, other native overheads, etc. This tends to grow with the driver's" +
        " memory size (typically 6-10%).")
      .bytesConf(ByteUnit.MiB)
      .createOptional

  private[spark] val KUBERNETES_DRIVER_LABELS =
    ConfigBuilder("spark.kubernetes.driver.labels")
      .doc("Custom labels that will be added to the driver pod. This should be a comma-separated" +
        " list of label key-value pairs, where each label is in the format key=value. Note that" +
        " Spark also adds its own labels to the driver pod for bookkeeping purposes.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_ANNOTATIONS =
    ConfigBuilder("spark.kubernetes.driver.annotations")
      .doc("Custom annotations that will be added to the driver pod. This should be a" +
        " comma-separated list of annotation key-value pairs, where each annotation is in the" +
        " format key=value.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_SUBMIT_TIMEOUT =
    ConfigBuilder("spark.kubernetes.driverSubmissionTimeout")
      .doc("Time to wait for the driver process to start running before aborting its execution.")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(60L)

  private[spark] val KUBERNETES_DRIVER_SUBMIT_SSL_KEYSTORE =
    ConfigBuilder("spark.ssl.kubernetes.driversubmitserver.keyStore")
      .doc("KeyStore file for the driver submission server listening on SSL. Can be pre-mounted" +
        " on the driver container or uploaded from the submitting client.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_SUBMIT_SSL_TRUSTSTORE =
    ConfigBuilder("spark.ssl.kubernetes.driversubmitserver.trustStore")
      .doc("TrustStore containing certificates for communicating to the driver submission server" +
        " over SSL.")
      .stringConf
      .createOptional

  private[spark] val DRIVER_SUBMIT_SSL_ENABLED =
    ConfigBuilder("spark.ssl.kubernetes.driversubmitserver.enabled")
      .doc("Whether or not to use SSL when sending the application dependencies to the driver pod.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val DRIVER_SUBMIT_SSL_KEY_PEM =
    ConfigBuilder("spark.ssl.kubernetes.driversubmitserver.keyPem")
      .doc("Key PEM file that the driver submission server will use when setting up TLS" +
        " connections. Can be pre-mounted on the driver pod's disk or uploaded from the" +
        " submitting client's machine.")
      .stringConf
      .createOptional

  private[spark] val DRIVER_SUBMIT_SSL_SERVER_CERT_PEM =
    ConfigBuilder("spark.ssl.kubernetes.driversubmitserver.serverCertPem")
      .doc("Certificate PEM file that is associated with the key PEM file" +
        " the submission server uses to set up TLS connections. Can be pre-mounted" +
        " on the driver pod's disk or uploaded from the submitting client's machine.")
      .stringConf
      .createOptional

  private[spark] val DRIVER_SUBMIT_SSL_CLIENT_CERT_PEM =
    ConfigBuilder("spark.ssl.kubernetes.driversubmitserver.clientCertPem")
      .doc("Certificate pem file that the submission client uses to connect to the submission" +
        " server over TLS. This should often be the same as the server certificate, but can be" +
        " different if the submission client will contact the driver through a proxy instead of" +
        " the driver service directly.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_SERVICE_NAME =
    ConfigBuilder("spark.kubernetes.driver.service.name")
        .doc("Kubernetes service that exposes the driver pod for external access.")
        .internal()
        .stringConf
        .createOptional

  private[spark] val KUBERNETES_DRIVER_SUBMIT_SERVER_MEMORY =
    ConfigBuilder("spark.kubernetes.driver.submissionServerMemory")
      .doc("The amount of memory to allocate for the driver submission server.")
      .bytesConf(ByteUnit.MiB)
      .createWithDefaultString("256m")

  private[spark] val EXPOSE_KUBERNETES_DRIVER_SERVICE_UI_PORT =
    ConfigBuilder("spark.kubernetes.driver.service.exposeUiPort")
      .doc("Whether to expose the driver Web UI port as a service NodePort. Turned off by default" +
        " because NodePort is a limited resource. Use alternatives if possible.")
      .booleanConf
      .createWithDefault(false)

  private[spark] val KUBERNETES_DRIVER_POD_NAME =
    ConfigBuilder("spark.kubernetes.driver.pod.name")
      .doc("Name of the driver pod.")
      .internal()
      .stringConf
      .createOptional

  private[spark] val DRIVER_SERVICE_MANAGER_TYPE =
    ConfigBuilder("spark.kubernetes.driver.serviceManagerType")
      .doc("A tag indicating which class to use for creating the Kubernetes service and" +
        " determining its URI for the submission client.")
      .stringConf
      .createWithDefault(NodePortUrisDriverServiceManager.TYPE)

  private[spark] val WAIT_FOR_APP_COMPLETION =
    ConfigBuilder("spark.kubernetes.submission.waitAppCompletion")
      .doc("In cluster mode, whether to wait for the application to finish before exiting the" +
        " launcher process.")
      .booleanConf
      .createWithDefault(true)

  private[spark] val REPORT_INTERVAL =
    ConfigBuilder("spark.kubernetes.report.interval")
      .doc("Interval between reports of the current app status in cluster mode.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1s")
}
