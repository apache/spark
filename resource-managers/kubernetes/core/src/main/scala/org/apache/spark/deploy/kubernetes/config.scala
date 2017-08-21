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
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

package object config extends Logging {

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

  private[spark] val DOCKER_IMAGE_PULL_POLICY =
    ConfigBuilder("spark.kubernetes.docker.image.pullPolicy")
      .doc("Docker image pull policy when pulling any docker image in Kubernetes integration")
      .stringConf
      .createWithDefault("IfNotPresent")

  private[spark] val APISERVER_AUTH_SUBMISSION_CONF_PREFIX =
      "spark.kubernetes.authenticate.submission"
  private[spark] val APISERVER_AUTH_DRIVER_CONF_PREFIX =
      "spark.kubernetes.authenticate.driver"
  private[spark] val APISERVER_AUTH_DRIVER_MOUNTED_CONF_PREFIX =
      "spark.kubernetes.authenticate.driver.mounted"
  private[spark] val APISERVER_AUTH_RESOURCE_STAGING_SERVER_CONF_PREFIX =
      "spark.kubernetes.authenticate.resourceStagingServer"
  private[spark] val APISERVER_AUTH_SHUFFLE_SERVICE_CONF_PREFIX =
      "spark.kubernetes.authenticate.shuffleService"
  private[spark] val OAUTH_TOKEN_CONF_SUFFIX = "oauthToken"
  private[spark] val OAUTH_TOKEN_FILE_CONF_SUFFIX = "oauthTokenFile"
  private[spark] val CLIENT_KEY_FILE_CONF_SUFFIX = "clientKeyFile"
  private[spark] val CLIENT_CERT_FILE_CONF_SUFFIX = "clientCertFile"
  private[spark] val CA_CERT_FILE_CONF_SUFFIX = "caCertFile"

  private[spark] val RESOURCE_STAGING_SERVER_USE_SERVICE_ACCOUNT_CREDENTIALS =
    ConfigBuilder(
          s"$APISERVER_AUTH_RESOURCE_STAGING_SERVER_CONF_PREFIX.useServiceAccountCredentials")
      .doc("Use a service account token and CA certificate in the resource staging server to" +
        " watch the API server's objects.")
      .booleanConf
      .createWithDefault(true)

  private[spark] val KUBERNETES_SERVICE_ACCOUNT_NAME =
    ConfigBuilder(s"$APISERVER_AUTH_DRIVER_CONF_PREFIX.serviceAccountName")
      .doc("Service account that is used when running the driver pod. The driver pod uses" +
        " this service account when requesting executor pods from the API server. If specific" +
        " credentials are given for the driver pod to use, the driver will favor" +
        " using those credentials instead.")
      .stringConf
      .createOptional

  private[spark] val SPARK_SHUFFLE_SERVICE_HOST =
    ConfigBuilder("spark.shuffle.service.host")
      .doc("Host for Spark Shuffle Service")
      .internal()
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

  private[spark] val KUBERNETES_DRIVER_LABEL_PREFIX = "spark.kubernetes.driver.label."
  private[spark] val KUBERNETES_DRIVER_ANNOTATION_PREFIX = "spark.kubernetes.driver.annotation."
  private[spark] val KUBERNETES_EXECUTOR_LABEL_PREFIX = "spark.kubernetes.executor.label."
  private[spark] val KUBERNETES_EXECUTOR_ANNOTATION_PREFIX = "spark.kubernetes.executor.annotation."

  private[spark] val KUBERNETES_DRIVER_LABELS =
    ConfigBuilder("spark.kubernetes.driver.labels")
      .doc("Custom labels that will be added to the driver pod. This should be a comma-separated" +
        " list of label key-value pairs, where each label is in the format key=value. Note that" +
        " Spark also adds its own labels to the driver pod for bookkeeping purposes.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_ENV_KEY = "spark.kubernetes.driverEnv."

  private[spark] val KUBERNETES_DRIVER_ANNOTATIONS =
    ConfigBuilder("spark.kubernetes.driver.annotations")
      .doc("Custom annotations that will be added to the driver pod. This should be a" +
        " comma-separated list of annotation key-value pairs, where each annotation is in the" +
        " format key=value.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_EXECUTOR_LABELS =
    ConfigBuilder("spark.kubernetes.executor.labels")
      .doc("Custom labels that will be added to the executor pods. This should be a" +
        " comma-separated list of label key-value pairs, where each label is in the format" +
        " key=value.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_EXECUTOR_ANNOTATIONS =
    ConfigBuilder("spark.kubernetes.executor.annotations")
      .doc("Custom annotations that will be added to the executor pods. This should be a" +
        " comma-separated list of annotation key-value pairs, where each annotation is in the" +
        " format key=value.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_POD_NAME =
    ConfigBuilder("spark.kubernetes.driver.pod.name")
      .doc("Name of the driver pod.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_EXECUTOR_POD_NAME_PREFIX =
    ConfigBuilder("spark.kubernetes.executor.podNamePrefix")
      .doc("Prefix to use in front of the executor pod names.")
      .internal()
      .stringConf
      .createWithDefault("spark")

  private[spark] val KUBERNETES_SHUFFLE_NAMESPACE =
    ConfigBuilder("spark.kubernetes.shuffle.namespace")
      .doc("Namespace of the shuffle service")
      .stringConf
      .createWithDefault("default")

  private[spark] val KUBERNETES_SHUFFLE_SVC_IP =
    ConfigBuilder("spark.kubernetes.shuffle.ip")
      .doc("This setting is for debugging only. Setting this " +
        "allows overriding the IP that the executor thinks its colocated " +
        "shuffle service is on")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_SHUFFLE_LABELS =
    ConfigBuilder("spark.kubernetes.shuffle.labels")
      .doc("Labels to identify the shuffle service")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_SHUFFLE_DIR =
    ConfigBuilder("spark.kubernetes.shuffle.dir")
      .doc("Path to the shared shuffle directories.")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_SHUFFLE_APISERVER_URI =
    ConfigBuilder("spark.kubernetes.shuffle.apiServer.url")
      .doc("URL to the Kubernetes API server that the shuffle service will monitor for Spark pods.")
      .stringConf
      .createWithDefault(KUBERNETES_MASTER_INTERNAL_URL)

  private[spark] val KUBERNETES_SHUFFLE_USE_SERVICE_ACCOUNT_CREDENTIALS =
    ConfigBuilder(s"$APISERVER_AUTH_SHUFFLE_SERVICE_CONF_PREFIX.useServiceAccountCredentials")
      .doc("Whether or not to use service account credentials when contacting the API server from" +
        " the shuffle service.")
      .booleanConf
      .createWithDefault(true)

  private[spark] val KUBERNETES_ALLOCATION_BATCH_SIZE =
    ConfigBuilder("spark.kubernetes.allocation.batch.size")
      .doc("Number of pods to launch at once in each round of dynamic allocation. ")
      .intConf
      .createWithDefault(5)

  private[spark] val KUBERNETES_ALLOCATION_BATCH_DELAY =
    ConfigBuilder("spark.kubernetes.allocation.batch.delay")
      .doc("Number of seconds to wait between each round of executor allocation. ")
      .longConf
      .createWithDefault(1)

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

  // Spark resource staging server.
  private[spark] val RESOURCE_STAGING_SERVER_API_SERVER_URL =
    ConfigBuilder("spark.kubernetes.resourceStagingServer.apiServer.url")
      .doc("URL for the Kubernetes API server. The resource staging server monitors the API" +
        " server to check when pods no longer are using mounted resources. Note that this isn't" +
        " to be used in Spark applications, as the API server URL should be set via spark.master.")
      .stringConf
      .createWithDefault(KUBERNETES_MASTER_INTERNAL_URL)

  private[spark] val RESOURCE_STAGING_SERVER_API_SERVER_CA_CERT_FILE =
    ConfigBuilder("spark.kubernetes.resourceStagingServer.apiServer.caCertFile")
      .doc("CA certificate for the resource staging server to use when contacting the Kubernetes" +
        " API server over TLS.")
      .stringConf
      .createOptional

  private[spark] val RESOURCE_STAGING_SERVER_PORT =
    ConfigBuilder("spark.kubernetes.resourceStagingServer.port")
      .doc("Port for the Kubernetes resource staging server to listen on.")
      .intConf
      .createWithDefault(10000)

  private[spark] val RESOURCE_STAGING_SERVER_INITIAL_ACCESS_EXPIRATION_TIMEOUT =
    ConfigBuilder("spark.kubernetes.resourceStagingServer.initialAccessExpirationTimeout")
      .doc("The resource staging server will wait for any resource bundle to be accessed for a" +
        " first time for this period. If this timeout expires before the resources are accessed" +
        " the first time, the resources are cleaned up under the assumption that the dependents" +
        " of the given resource bundle failed to launch at all.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30m")

  private[spark] val RESOURCE_STAGING_SERVER_KEY_PEM =
    ConfigBuilder("spark.ssl.kubernetes.resourceStagingServer.keyPem")
      .doc("Key PEM file to use when having the Kubernetes dependency server listen on TLS.")
      .stringConf
      .createOptional

  private[spark] val RESOURCE_STAGING_SERVER_SSL_NAMESPACE = "kubernetes.resourceStagingServer"
  private[spark] val RESOURCE_STAGING_SERVER_INTERNAL_SSL_NAMESPACE =
      "kubernetes.resourceStagingServer.internal"
  private[spark] val RESOURCE_STAGING_SERVER_CERT_PEM =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_SSL_NAMESPACE.serverCertPem")
      .doc("Certificate PEM file to use when having the resource staging server" +
        " listen on TLS.")
      .stringConf
      .createOptional
  private[spark] val RESOURCE_STAGING_SERVER_CLIENT_CERT_PEM =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_SSL_NAMESPACE.clientCertPem")
      .doc("Certificate PEM file to use when the client contacts the resource staging server." +
        " This must strictly be a path to a file on the submitting machine's disk.")
      .stringConf
      .createOptional
  private[spark] val RESOURCE_STAGING_SERVER_INTERNAL_CLIENT_CERT_PEM =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_INTERNAL_SSL_NAMESPACE.clientCertPem")
      .doc("Certificate PEM file to use when the init-container contacts the resource staging" +
        " server. If this is not provided, it defaults to the value of" +
        " spark.ssl.kubernetes.resourceStagingServer.clientCertPem. This can be a URI with" +
        " a scheme of local:// which denotes that the file is pre-mounted on the init-container's" +
        " disk. A uri without a scheme or a scheme of file:// will result in this file being" +
        " mounted from the submitting machine's disk as a secret into the pods.")
      .stringConf
      .createOptional
  private[spark] val RESOURCE_STAGING_SERVER_KEYSTORE_PASSWORD_FILE =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_SSL_NAMESPACE.keyStorePasswordFile")
      .doc("File containing the keystore password for the Kubernetes resource staging server.")
      .stringConf
      .createOptional

  private[spark] val RESOURCE_STAGING_SERVER_KEYSTORE_KEY_PASSWORD_FILE =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_SSL_NAMESPACE.keyPasswordFile")
      .doc("File containing the key password for the Kubernetes resource staging server.")
      .stringConf
      .createOptional

  private[spark] val RESOURCE_STAGING_SERVER_SSL_ENABLED =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_SSL_NAMESPACE.enabled")
      .doc("Whether or not to use SSL when communicating with the resource staging server.")
      .booleanConf
      .createOptional
  private[spark] val RESOURCE_STAGING_SERVER_INTERNAL_SSL_ENABLED =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_INTERNAL_SSL_NAMESPACE.enabled")
      .doc("Whether or not to use SSL when communicating with the resource staging server from" +
        " the init-container. If this is not provided, defaults to" +
        " the value of spark.ssl.kubernetes.resourceStagingServer.enabled")
      .booleanConf
      .createOptional
  private[spark] val RESOURCE_STAGING_SERVER_TRUSTSTORE_FILE =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_SSL_NAMESPACE.trustStore")
      .doc("File containing the trustStore to communicate with the Kubernetes dependency server." +
        " This must strictly be a path on the submitting machine's disk.")
      .stringConf
      .createOptional
  private[spark] val RESOURCE_STAGING_SERVER_INTERNAL_TRUSTSTORE_FILE =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_INTERNAL_SSL_NAMESPACE.trustStore")
      .doc("File containing the trustStore to communicate with the Kubernetes dependency server" +
        " from the init-container. If this is not provided, defaults to the value of" +
        " spark.ssl.kubernetes.resourceStagingServer.trustStore. This can be a URI with a scheme" +
        " of local:// indicating that the trustStore is pre-mounted on the init-container's" +
        " disk. If no scheme, or a scheme of file:// is provided, this file is mounted from the" +
        " submitting machine's disk as a Kubernetes secret into the pods.")
      .stringConf
      .createOptional
  private[spark] val RESOURCE_STAGING_SERVER_TRUSTSTORE_PASSWORD =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_SSL_NAMESPACE.trustStorePassword")
      .doc("Password for the trustStore for communicating to the dependency server.")
      .stringConf
      .createOptional
  private[spark] val RESOURCE_STAGING_SERVER_INTERNAL_TRUSTSTORE_PASSWORD =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_INTERNAL_SSL_NAMESPACE.trustStorePassword")
      .doc("Password for the trustStore for communicating to the dependency server from the" +
        " init-container. If this is not provided, defaults to" +
        " spark.ssl.kubernetes.resourceStagingServer.trustStorePassword.")
      .stringConf
      .createOptional
  private[spark] val RESOURCE_STAGING_SERVER_TRUSTSTORE_TYPE =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_SSL_NAMESPACE.trustStoreType")
      .doc("Type of trustStore for communicating with the dependency server.")
      .stringConf
      .createOptional
  private[spark] val RESOURCE_STAGING_SERVER_INTERNAL_TRUSTSTORE_TYPE =
    ConfigBuilder(s"spark.ssl.$RESOURCE_STAGING_SERVER_INTERNAL_SSL_NAMESPACE.trustStoreType")
      .doc("Type of trustStore for communicating with the dependency server from the" +
        " init-container. If this is not provided, defaults to" +
        " spark.ssl.kubernetes.resourceStagingServer.trustStoreType")
      .stringConf
      .createOptional

  // Driver and Init-Container parameters
  private[spark] val RESOURCE_STAGING_SERVER_URI =
    ConfigBuilder("spark.kubernetes.resourceStagingServer.uri")
      .doc("Base URI for the Spark resource staging server.")
      .stringConf
      .createOptional

  private[spark] val RESOURCE_STAGING_SERVER_INTERNAL_URI =
    ConfigBuilder("spark.kubernetes.resourceStagingServer.internal.uri")
      .doc("Base URI for the Spark resource staging server when the init-containers access it for" +
        " downloading resources. If this is not provided, it defaults to the value provided in" +
        " spark.kubernetes.resourceStagingServer.uri, the URI that the submission client uses to" +
        " upload the resources from outside the cluster.")
      .stringConf
      .createOptional

  private[spark] val INIT_CONTAINER_DOWNLOAD_JARS_RESOURCE_IDENTIFIER =
    ConfigBuilder("spark.kubernetes.initcontainer.downloadJarsResourceIdentifier")
      .doc("Identifier for the jars tarball that was uploaded to the staging service.")
      .internal()
      .stringConf
      .createOptional

  private[spark] val INIT_CONTAINER_DOWNLOAD_JARS_SECRET_LOCATION =
    ConfigBuilder("spark.kubernetes.initcontainer.downloadJarsSecretLocation")
      .doc("Location of the application secret to use when the init-container contacts the" +
        " resource staging server to download jars.")
      .internal()
      .stringConf
      .createWithDefault(s"$INIT_CONTAINER_SECRET_VOLUME_MOUNT_PATH/" +
        s"$INIT_CONTAINER_SUBMITTED_JARS_SECRET_KEY")

  private[spark] val INIT_CONTAINER_DOWNLOAD_FILES_RESOURCE_IDENTIFIER =
    ConfigBuilder("spark.kubernetes.initcontainer.downloadFilesResourceIdentifier")
      .doc("Identifier for the files tarball that was uploaded to the staging service.")
      .internal()
      .stringConf
      .createOptional

  private[spark] val INIT_CONTAINER_DOWNLOAD_FILES_SECRET_LOCATION =
    ConfigBuilder("spark.kubernetes.initcontainer.downloadFilesSecretLocation")
      .doc("Location of the application secret to use when the init-container contacts the" +
        " resource staging server to download files.")
      .internal()
      .stringConf
      .createWithDefault(
        s"$INIT_CONTAINER_SECRET_VOLUME_MOUNT_PATH/$INIT_CONTAINER_SUBMITTED_FILES_SECRET_KEY")

  private[spark] val INIT_CONTAINER_REMOTE_JARS =
    ConfigBuilder("spark.kubernetes.initcontainer.remoteJars")
      .doc("Comma-separated list of jar URIs to download in the init-container. This is" +
        " calculated from spark.jars.")
      .internal()
      .stringConf
      .createOptional

  private[spark] val INIT_CONTAINER_REMOTE_FILES =
    ConfigBuilder("spark.kubernetes.initcontainer.remoteFiles")
      .doc("Comma-separated list of file URIs to download in the init-container. This is" +
        " calculated from spark.files.")
      .internal()
      .stringConf
      .createOptional

  private[spark] val INIT_CONTAINER_DOCKER_IMAGE =
    ConfigBuilder("spark.kubernetes.initcontainer.docker.image")
      .doc("Image for the driver and executor's init-container that downloads dependencies.")
      .stringConf
      .createWithDefault(s"spark-init:$sparkVersion")

  private[spark] val INIT_CONTAINER_JARS_DOWNLOAD_LOCATION =
    ConfigBuilder("spark.kubernetes.mountdependencies.jarsDownloadDir")
      .doc("Location to download jars to in the driver and executors. When using" +
        " spark-submit, this directory must be empty and will be mounted as an empty directory" +
        " volume on the driver and executor pod.")
      .stringConf
      .createWithDefault("/var/spark-data/spark-jars")

  private[spark] val INIT_CONTAINER_FILES_DOWNLOAD_LOCATION =
    ConfigBuilder("spark.kubernetes.mountdependencies.filesDownloadDir")
      .doc("Location to download files to in the driver and executors. When using" +
        " spark-submit, this directory must be empty and will be mounted as an empty directory" +
        " volume on the driver and executor pods.")
      .stringConf
      .createWithDefault("/var/spark-data/spark-files")

  private[spark] val INIT_CONTAINER_MOUNT_TIMEOUT =
    ConfigBuilder("spark.kubernetes.mountdependencies.mountTimeout")
      .doc("Timeout before aborting the attempt to download and unpack local dependencies from" +
        " remote locations and the resource staging server when initializing the driver and" +
        " executor pods.")
      .timeConf(TimeUnit.MINUTES)
      .createWithDefault(5)

  private[spark] val EXECUTOR_INIT_CONTAINER_CONFIG_MAP =
    ConfigBuilder("spark.kubernetes.initcontainer.executor.configmapname")
      .doc("Name of the config map to use in the init-container that retrieves submitted files" +
        " for the executor.")
      .internal()
      .stringConf
      .createOptional

  private[spark] val EXECUTOR_INIT_CONTAINER_CONFIG_MAP_KEY =
    ConfigBuilder("spark.kubernetes.initcontainer.executor.configmapkey")
      .doc("Key for the entry in the init container config map for submitted files that" +
        " corresponds to the properties for this init-container.")
      .internal()
      .stringConf
      .createOptional

  private[spark] val EXECUTOR_INIT_CONTAINER_SECRET =
    ConfigBuilder("spark.kubernetes.initcontainer.executor.stagingServerSecret.name")
      .doc("Name of the secret to mount into the init-container that retrieves submitted files.")
      .internal()
      .stringConf
      .createOptional

  private[spark] val EXECUTOR_INIT_CONTAINER_SECRET_MOUNT_DIR =
    ConfigBuilder("spark.kubernetes.initcontainer.executor.stagingServerSecret.mountDir")
      .doc("Directory to mount the resource staging server secrets into for the executor" +
        " init-containers. This must be exactly the same as the directory that the submission" +
        " client mounted the secret into because the config map's properties specify the" +
        " secret location as to be the same between the driver init-container and the executor" +
        " init-container. Thus the submission client will always set this and the driver will" +
        " never rely on a constant or convention, in order to protect against cases where the" +
        " submission client has a different version from the driver itself, and hence might" +
        " have different constants loaded in constants.scala.")
      .internal()
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_LIMIT_CORES =
    ConfigBuilder("spark.kubernetes.driver.limit.cores")
      .doc("Specify the hard cpu limit for the driver pod")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_DRIVER_CLUSTER_NODENAME_DNS_LOOKUP_ENABLED =
    ConfigBuilder("spark.kubernetes.driver.hdfslocality.clusterNodeNameDNSLookup.enabled")
      .doc("Whether or not HDFS locality support code should look up DNS for full hostnames of" +
        " cluster nodes. In some K8s clusters, notably GKE, cluster node names are short" +
        " hostnames, and so comparing them against HDFS datanode hostnames always fail. To fix," +
        " enable this flag. This is disabled by default because DNS lookup can be expensive." +
        " The driver can slow down and fail to respond to executor heartbeats in time." +
        " If enabling this flag, make sure your DNS server has enough capacity" +
        " for the workload.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  private[spark] val KUBERNETES_EXECUTOR_LIMIT_CORES =
    ConfigBuilder("spark.kubernetes.executor.limit.cores")
      .doc("Specify the hard cpu limit for a single executor pod")
      .stringConf
      .createOptional

  private[spark] val KUBERNETES_NODE_SELECTOR_PREFIX = "spark.kubernetes.node.selector."

  private[spark] def resolveK8sMaster(rawMasterString: String): String = {
    if (!rawMasterString.startsWith("k8s://")) {
      throw new IllegalArgumentException("Master URL should start with k8s:// in Kubernetes mode.")
    }
    val masterWithoutK8sPrefix = rawMasterString.replaceFirst("k8s://", "")
    if (masterWithoutK8sPrefix.startsWith("http://")
      || masterWithoutK8sPrefix.startsWith("https://")) {
      masterWithoutK8sPrefix
    } else {
      val resolvedURL = s"https://$masterWithoutK8sPrefix"
      logDebug(s"No scheme specified for kubernetes master URL, so defaulting to https. Resolved" +
        s" URL is $resolvedURL")
      resolvedURL
    }
  }
}
