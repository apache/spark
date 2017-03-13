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
import org.apache.spark.deploy.rest.kubernetes.NodePortUrisDriverServiceManager
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

package object config {

  private[spark] val KUBERNETES_NAMESPACE =
    ConfigBuilder("spark.kubernetes.namespace")
      .doc("""
          | The namespace that will be used for running the driver and
          | executor pods. When using spark-submit in cluster mode,
          | this can also be passed to spark-submit via the
          | --kubernetes-namespace command line argument.
        """.stripMargin)
      .stringConf
      .createWithDefault("default")

  private[spark] val DRIVER_DOCKER_IMAGE =
    ConfigBuilder("spark.kubernetes.driver.docker.image")
      .doc("""
          | Docker image to use for the driver. Specify this using the
          | standard Docker tag format.
        """.stripMargin)
      .stringConf
      .createWithDefault(s"spark-driver:$sparkVersion")

  private[spark] val EXECUTOR_DOCKER_IMAGE =
    ConfigBuilder("spark.kubernetes.executor.docker.image")
      .doc("""
          | Docker image to use for the executors. Specify this using
          | the standard Docker tag format.
        """.stripMargin)
      .stringConf
      .createWithDefault(s"spark-executor:$sparkVersion")

  private[spark] val KUBERNETES_CA_CERT_FILE =
    ConfigBuilder("spark.kubernetes.submit.caCertFile")
      .doc("""
          | CA cert file for connecting to Kubernetes over SSL. This
          | file should be located on the submitting machine's disk.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_CLIENT_KEY_FILE =
    ConfigBuilder("spark.kubernetes.submit.clientKeyFile")
      .doc("""
          | Client key file for authenticating against the Kubernetes
          | API server. This file should be located on the submitting
          | machine's disk.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_CLIENT_CERT_FILE =
    ConfigBuilder("spark.kubernetes.submit.clientCertFile")
      .doc("""
          | Client cert file for authenticating against the
          | Kubernetes API server. This file should be located on
          | the submitting machine's disk.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_OAUTH_TOKEN =
    ConfigBuilder("spark.kubernetes.submit.oauthToken")
      .doc("""
          | OAuth token to use when authenticating against the
          | against the Kubernetes API server. Note that unlike
          | the other authentication options, this should be the
          | exact string value of the token to use for the
          | authentication.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_SERVICE_ACCOUNT_NAME =
    ConfigBuilder("spark.kubernetes.submit.serviceAccountName")
      .doc("""
          | Service account that is used when running the driver pod.
          | The driver pod uses this service account when requesting
          | executor pods from the API server.
        """.stripMargin)
      .stringConf
      .createWithDefault("default")

  // Note that while we set a default for this when we start up the
  // scheduler, the specific default value is dynamically determined
  // based on the executor memory.
  private[spark] val KUBERNETES_EXECUTOR_MEMORY_OVERHEAD =
    ConfigBuilder("spark.kubernetes.executor.memoryOverhead")
      .doc("""
          | The amount of off-heap memory (in megabytes) to be
          | allocated per executor. This is memory that accounts for
          | things like VM overheads, interned strings, other native
          | overheads, etc. This tends to grow with the executor size
          | (typically 6-10%).
        """.stripMargin)
      .bytesConf(ByteUnit.MiB)
      .createOptional

  private[spark] val KUBERNETES_DRIVER_MEMORY_OVERHEAD =
    ConfigBuilder("spark.kubernetes.driver.memoryOverhead")
      .doc("""
          | The amount of off-heap memory (in megabytes) to be
          | allocated for the driver and the driver submission server.
          | This is memory that accounts for things like VM overheads,
          | interned strings, other native overheads, etc. This tends
          | to grow with the driver's memory size (typically 6-10%).
           """.stripMargin)
      .bytesConf(ByteUnit.MiB)
      .createOptional

  private[spark] val KUBERNETES_DRIVER_LABELS =
    ConfigBuilder("spark.kubernetes.driver.labels")
      .doc("""
          | Custom labels that will be added to the driver pod.
          | This should be a comma-separated list of label key-value
          | pairs, where each label is in the format key=value. Note
          | that Spark also adds its own labels to the driver pod
          | for bookkeeping purposes.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_ANNOTATIONS =
    ConfigBuilder("spark.kubernetes.driver.annotations")
      .doc("""
             | Custom annotations that will be added to the driver pod.
             | This should be a comma-separated list of annotation key-value
             | pairs, where each annotation is in the format key=value.
           """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_SUBMIT_TIMEOUT =
    ConfigBuilder("spark.kubernetes.driverSubmitTimeout")
      .doc("""
          | Time to wait for the driver process to start running
          | before aborting its execution.
        """.stripMargin)
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(60L)

  private[spark] val KUBERNETES_DRIVER_SUBMIT_KEYSTORE =
    ConfigBuilder("spark.ssl.kubernetes.submit.keyStore")
      .doc("""
          | KeyStore file for the driver submission server listening
          | on SSL. Can be pre-mounted on the driver container
          | or uploaded from the submitting client.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_SUBMIT_TRUSTSTORE =
    ConfigBuilder("spark.ssl.kubernetes.submit.trustStore")
      .doc("""
          | TrustStore containing certificates for communicating
          | to the driver submission server over SSL.
        """.stripMargin)
      .stringConf
      .createOptional

  private[spark] val DRIVER_SUBMIT_SSL_ENABLED =
    ConfigBuilder("spark.ssl.kubernetes.submit.enabled")
      .doc("""
             | Whether or not to use SSL when sending the
             | application dependencies to the driver pod.
             |
           """.stripMargin)
      .booleanConf
      .createWithDefault(false)

  private[spark] val KUBERNETES_DRIVER_SERVICE_NAME =
    ConfigBuilder("spark.kubernetes.driver.service.name")
        .doc("""
            | Kubernetes service that exposes the driver pod
            | for external access.
          """.stripMargin)
        .internal()
        .stringConf
        .createOptional

  private[spark] val KUBERNETES_DRIVER_SUBMIT_SERVER_MEMORY =
    ConfigBuilder("spark.kubernetes.driver.submissionServerMemory")
      .doc("""
          | The amount of memory to allocate for the driver submission server.
        """.stripMargin)
      .bytesConf(ByteUnit.MiB)
      .createWithDefaultString("256m")

  private[spark] val EXPOSE_KUBERNETES_DRIVER_SERVICE_UI_PORT =
    ConfigBuilder("spark.kubernetes.driver.service.exposeUiPort")
      .doc("""
          | Whether to expose the driver Web UI port as a service NodePort. Turned off by default
          | because NodePort is a limited resource. Use alternatives such as Ingress if possible.
        """.stripMargin)
      .booleanConf
      .createWithDefault(false)

  private[spark] val KUBERNETES_DRIVER_POD_NAME =
    ConfigBuilder("spark.kubernetes.driver.pod.name")
      .doc("""
          | Name of the driver pod.
        """.stripMargin)
      .internal()
      .stringConf
      .createOptional

  private[spark] val DRIVER_SERVICE_MANAGER_TYPE =
    ConfigBuilder("spark.kubernetes.driver.serviceManagerType")
      .doc(s"""
          | A tag indicating which class to use for creating the
          | Kubernetes service and determining its URI for the submission
          | client.
        """.stripMargin)
      .stringConf
      .createWithDefault(NodePortUrisDriverServiceManager.TYPE)

  private[spark] val WAIT_FOR_APP_COMPLETION =
    ConfigBuilder("spark.kubernetes.submit.waitAppCompletion")
      .doc(
        """
          | In cluster mode, whether to wait for the application to finish before exiting the
          | launcher process.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  private[spark] val REPORT_INTERVAL =
    ConfigBuilder("spark.kubernetes.report.interval")
      .doc(
        """
          | Interval between reports of the current app status in cluster mode.
        """.stripMargin)
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1s")
}
