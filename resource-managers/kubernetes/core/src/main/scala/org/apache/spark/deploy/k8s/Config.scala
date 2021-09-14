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

import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{PYSPARK_DRIVER_PYTHON, PYSPARK_PYTHON}
import org.apache.spark.internal.config.ConfigBuilder

private[spark] object Config extends Logging {

  val DECOMMISSION_SCRIPT =
    ConfigBuilder("spark.kubernetes.decommission.script")
      .doc("The location of the script to use for graceful decommissioning")
      .version("3.2.0")
      .stringConf
      .createWithDefault("/opt/decom.sh")


  val KUBERNETES_CONTEXT =
    ConfigBuilder("spark.kubernetes.context")
      .doc("The desired context from your K8S config file used to configure the K8S " +
        "client for interacting with the cluster.  Useful if your config file has " +
        "multiple clusters or user identities defined.  The client library used " +
        "locates the config file via the KUBECONFIG environment variable or by defaulting " +
        "to .kube/config under your home directory.  If not specified then your current " +
        "context is used.  You can always override specific aspects of the config file " +
        "provided configuration using other Spark on K8S configuration options.")
      .version("3.0.0")
      .stringConf
      .createOptional

  val KUBERNETES_DRIVER_MASTER_URL =
    ConfigBuilder("spark.kubernetes.driver.master")
      .doc("The internal Kubernetes master (API server) address " +
        "to be used for driver to request executors.")
      .version("3.0.0")
      .stringConf
      .createWithDefault(KUBERNETES_MASTER_INTERNAL_URL)

  val KUBERNETES_DRIVER_SERVICE_DELETE_ON_TERMINATION =
    ConfigBuilder("spark.kubernetes.driver.service.deleteOnTermination")
      .doc("If true, driver service will be deleted on Spark application termination. " +
        "If false, it will be cleaned up when the driver pod is deletion.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(true)

  val KUBERNETES_DRIVER_OWN_PVC =
    ConfigBuilder("spark.kubernetes.driver.ownPersistentVolumeClaim")
      .doc("If true, driver pod becomes the owner of on-demand persistent volume claims " +
        "instead of the executor pods")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(false)

  val KUBERNETES_DRIVER_REUSE_PVC =
    ConfigBuilder("spark.kubernetes.driver.reusePersistentVolumeClaim")
      .doc("If true, driver pod tries to reuse driver-owned on-demand persistent volume claims " +
        "of the deleted executor pods if exists. This can be useful to reduce executor pod " +
        "creation delay by skipping persistent volume creations. Note that a pod in " +
        "`Terminating` pod status is not a deleted pod by definition and its resources " +
        "including persistent volume claims are not reusable yet. Spark will create new " +
        "persistent volume claims when there exists no reusable one. In other words, the total " +
        "number of persistent volume claims can be larger than the number of running executors " +
        s"sometimes. This config requires ${KUBERNETES_DRIVER_OWN_PVC.key}=true.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(false)

  val KUBERNETES_NAMESPACE =
    ConfigBuilder("spark.kubernetes.namespace")
      .doc("The namespace that will be used for running the driver and executor pods.")
      .version("2.3.0")
      .stringConf
      .createWithDefault("default")

  val CONTAINER_IMAGE =
    ConfigBuilder("spark.kubernetes.container.image")
      .doc("Container image to use for Spark containers. Individual container types " +
        "(e.g. driver or executor) can also be configured to use different images if desired, " +
        "by setting the container type-specific image name.")
      .version("2.3.0")
      .stringConf
      .createOptional

  val DRIVER_CONTAINER_IMAGE =
    ConfigBuilder("spark.kubernetes.driver.container.image")
      .doc("Container image to use for the driver.")
      .version("2.3.0")
      .fallbackConf(CONTAINER_IMAGE)

  val EXECUTOR_CONTAINER_IMAGE =
    ConfigBuilder("spark.kubernetes.executor.container.image")
      .doc("Container image to use for the executors.")
      .version("2.3.0")
      .fallbackConf(CONTAINER_IMAGE)

  val CONTAINER_IMAGE_PULL_POLICY =
    ConfigBuilder("spark.kubernetes.container.image.pullPolicy")
      .doc("Kubernetes image pull policy. Valid values are Always, Never, and IfNotPresent.")
      .version("2.3.0")
      .stringConf
      .checkValues(Set("Always", "Never", "IfNotPresent"))
      .createWithDefault("IfNotPresent")

  val IMAGE_PULL_SECRETS =
    ConfigBuilder("spark.kubernetes.container.image.pullSecrets")
      .doc("Comma separated list of the Kubernetes secrets used " +
        "to access private image registries.")
      .version("2.4.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  val CONFIG_MAP_MAXSIZE =
    ConfigBuilder("spark.kubernetes.configMap.maxSize")
      .doc("Max size limit for a config map. This is configurable as per" +
        " https://etcd.io/docs/v3.4.0/dev-guide/limit/ on k8s server end.")
      .version("3.1.0")
      .longConf
      .createWithDefault(1572864) // 1.5 MiB

  val KUBERNETES_AUTH_DRIVER_CONF_PREFIX = "spark.kubernetes.authenticate.driver"
  val KUBERNETES_AUTH_EXECUTOR_CONF_PREFIX = "spark.kubernetes.authenticate.executor"
  val KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX = "spark.kubernetes.authenticate.driver.mounted"
  val KUBERNETES_AUTH_CLIENT_MODE_PREFIX = "spark.kubernetes.authenticate"
  val OAUTH_TOKEN_CONF_SUFFIX = "oauthToken"
  val OAUTH_TOKEN_FILE_CONF_SUFFIX = "oauthTokenFile"
  val CLIENT_KEY_FILE_CONF_SUFFIX = "clientKeyFile"
  val CLIENT_CERT_FILE_CONF_SUFFIX = "clientCertFile"
  val CA_CERT_FILE_CONF_SUFFIX = "caCertFile"

  val SUBMISSION_CLIENT_REQUEST_TIMEOUT =
    ConfigBuilder("spark.kubernetes.submission.requestTimeout")
      .doc("request timeout to be used in milliseconds for starting the driver")
      .version("3.0.0")
      .intConf
      .createWithDefault(10000)

  val SUBMISSION_CLIENT_CONNECTION_TIMEOUT =
    ConfigBuilder("spark.kubernetes.submission.connectionTimeout")
      .doc("connection timeout to be used in milliseconds for starting the driver")
      .version("3.0.0")
      .intConf
      .createWithDefault(10000)

  val DRIVER_CLIENT_REQUEST_TIMEOUT =
    ConfigBuilder("spark.kubernetes.driver.requestTimeout")
      .doc("request timeout to be used in milliseconds for driver to request executors")
      .version("3.0.0")
      .intConf
      .createWithDefault(10000)

  val DRIVER_CLIENT_CONNECTION_TIMEOUT =
    ConfigBuilder("spark.kubernetes.driver.connectionTimeout")
      .doc("connection timeout to be used in milliseconds for driver to request executors")
      .version("3.0.0")
      .intConf
      .createWithDefault(10000)

  val KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME =
    ConfigBuilder(s"$KUBERNETES_AUTH_DRIVER_CONF_PREFIX.serviceAccountName")
      .doc("Service account that is used when running the driver pod. The driver pod uses " +
        "this service account when requesting executor pods from the API server. If specific " +
        "credentials are given for the driver pod to use, the driver will favor " +
        "using those credentials instead.")
      .version("2.3.0")
      .stringConf
      .createOptional

  val KUBERNETES_EXECUTOR_SERVICE_ACCOUNT_NAME =
    ConfigBuilder(s"$KUBERNETES_AUTH_EXECUTOR_CONF_PREFIX.serviceAccountName")
      .doc("Service account that is used when running the executor pod." +
        "If this parameter is not setup, the fallback logic will use the driver's service account.")
      .version("3.1.0")
      .stringConf
      .createOptional

  val KUBERNETES_DRIVER_LIMIT_CORES =
    ConfigBuilder("spark.kubernetes.driver.limit.cores")
      .doc("Specify the hard cpu limit for the driver pod")
      .version("2.3.0")
      .stringConf
      .createOptional

  val KUBERNETES_DRIVER_REQUEST_CORES =
    ConfigBuilder("spark.kubernetes.driver.request.cores")
      .doc("Specify the cpu request for the driver pod")
      .version("3.0.0")
      .stringConf
      .createOptional

  val KUBERNETES_DRIVER_SUBMIT_CHECK =
    ConfigBuilder("spark.kubernetes.submitInDriver")
    .internal()
    .version("2.4.0")
    .booleanConf
    .createWithDefault(false)

  val KUBERNETES_EXECUTOR_LIMIT_CORES =
    ConfigBuilder("spark.kubernetes.executor.limit.cores")
      .doc("Specify the hard cpu limit for each executor pod")
      .version("2.3.0")
      .stringConf
      .createOptional

  val KUBERNETES_EXECUTOR_SCHEDULER_NAME =
    ConfigBuilder("spark.kubernetes.executor.scheduler.name")
      .doc("Specify the scheduler name for each executor pod")
      .version("3.0.0")
      .stringConf
      .createOptional

  val KUBERNETES_EXECUTOR_REQUEST_CORES =
    ConfigBuilder("spark.kubernetes.executor.request.cores")
      .doc("Specify the cpu request for each executor pod")
      .version("2.4.0")
      .stringConf
      .createOptional

  val KUBERNETES_DRIVER_POD_NAME =
    ConfigBuilder("spark.kubernetes.driver.pod.name")
      .doc("Name of the driver pod.")
      .version("2.3.0")
      .stringConf
      .createOptional

  // For testing only.
  val KUBERNETES_DRIVER_POD_NAME_PREFIX =
    ConfigBuilder("spark.kubernetes.driver.resourceNamePrefix")
      .internal()
      .version("3.0.0")
      .stringConf
      .createOptional

  // the definition of a label in DNS (RFC 1123).
  private val dns1123LabelFmt = "[a-z0-9]([-a-z0-9]*[a-z0-9])?"

  private val podConfValidator = (s"^$dns1123LabelFmt(\\.$dns1123LabelFmt)*$$").r.pattern

  // The possible longest executor name would be "$prefix-exec-${Int.MaxValue}"
  private def isValidExecutorPodNamePrefix(prefix: String): Boolean = {
    // 6 is length of '-exec-'
    val reservedLen = Int.MaxValue.toString.length + 6
    val validLength = prefix.length + reservedLen <= KUBERNETES_DNSNAME_MAX_LENGTH
    validLength && podConfValidator.matcher(prefix).matches()
  }

  val KUBERNETES_EXECUTOR_POD_NAME_PREFIX =
    ConfigBuilder("spark.kubernetes.executor.podNamePrefix")
      .doc("Prefix to use in front of the executor pod names. It must conform the rules defined " +
        "by the Kubernetes <a href=\"https://kubernetes.io/docs/concepts/overview/" +
        "working-with-objects/names/#dns-label-names\">DNS Label Names</a>. " +
        "The prefix will be used to generate executor pod names in the form of " +
        "<code>$podNamePrefix-exec-$id</code>, where the `id` is a positive int value, " +
        "so the length of the `podNamePrefix` needs to be <= 47(= 63 - 10 - 6).")
      .version("2.3.0")
      .stringConf
      .checkValue(isValidExecutorPodNamePrefix,
        "must conform https://kubernetes.io/docs/concepts/overview/working-with-objects" +
          "/names/#dns-label-names and the value length <= 47")
      .createOptional

  val KUBERNETES_EXECUTOR_DISABLE_CONFIGMAP =
    ConfigBuilder("spark.kubernetes.executor.disableConfigMap")
      .doc("If true, disable ConfigMap creation for executors.")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(false)

  val KUBERNETES_DRIVER_POD_FEATURE_STEPS =
    ConfigBuilder("spark.kubernetes.driver.pod.featureSteps")
      .doc("Class names of an extra driver pod feature step implementing " +
        "KubernetesFeatureConfigStep. This is a developer API. Comma separated. " +
        "Runs after all of Spark internal feature steps.")
      .version("3.2.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  val KUBERNETES_EXECUTOR_POD_FEATURE_STEPS =
    ConfigBuilder("spark.kubernetes.executor.pod.featureSteps")
      .doc("Class name of an extra executor pod feature step implementing " +
        "KubernetesFeatureConfigStep. This is a developer API. Comma separated. " +
        "Runs after all of Spark internal feature steps.")
      .version("3.2.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  val KUBERNETES_EXECUTOR_DECOMMISSION_LABEL =
    ConfigBuilder("spark.kubernetes.executor.decommmissionLabel")
      .doc("Label to apply to a pod which is being decommissioned." +
        " Designed for use with pod disruption budgets and similar mechanism" +
        " such as pod-deletion-cost.")
      .version("3.3.0")
      .stringConf
      .createOptional

  val KUBERNETES_EXECUTOR_DECOMMISSION_LABEL_VALUE =
    ConfigBuilder("spark.kubernetes.executor.decommmissionLabelValue")
      .doc("Label value to apply to a pod which is being decommissioned." +
        " Designed for use with pod disruption budgets and similar mechanism" +
        " such as pod-deletion-cost.")
      .version("3.3.0")
      .stringConf
      .createOptional

  val KUBERNETES_ALLOCATION_PODS_ALLOCATOR =
    ConfigBuilder("spark.kubernetes.allocation.pods.allocator")
      .doc("Allocator to use for pods. Possible values are direct (the default) and statefulset " +
        ", or a full class name of a class implementing AbstractPodsAllocator. " +
        "Future version may add Job or replicaset. This is a developer API and may change " +
      "or be removed at anytime.")
      .version("3.3.0")
      .stringConf
      .createWithDefault("direct")

  val KUBERNETES_ALLOCATION_BATCH_SIZE =
    ConfigBuilder("spark.kubernetes.allocation.batch.size")
      .doc("Number of pods to launch at once in each round of executor allocation.")
      .version("2.3.0")
      .intConf
      .checkValue(value => value > 0, "Allocation batch size should be a positive integer")
      .createWithDefault(5)

  val KUBERNETES_ALLOCATION_BATCH_DELAY =
    ConfigBuilder("spark.kubernetes.allocation.batch.delay")
      .doc("Time to wait between each round of executor allocation.")
      .version("2.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(value => value > 0, "Allocation batch delay must be a positive time value.")
      .createWithDefaultString("1s")

  val KUBERNETES_ALLOCATION_DRIVER_READINESS_TIMEOUT =
    ConfigBuilder("spark.kubernetes.allocation.driver.readinessTimeout")
      .doc("Time to wait for driver pod to get ready before creating executor pods. This wait " +
        "only happens on application start. If timeout happens, executor pods will still be " +
        "created.")
      .version("3.1.3")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(value => value > 0, "Allocation driver readiness timeout must be a positive "
        + "time value.")
      .createWithDefaultString("1s")

  val KUBERNETES_ALLOCATION_EXECUTOR_TIMEOUT =
    ConfigBuilder("spark.kubernetes.allocation.executor.timeout")
      .doc("Time to wait before a newly created executor POD request, which does not reached " +
        "the POD pending state yet, considered timedout and will be deleted.")
      .version("3.1.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(value => value > 0, "Allocation executor timeout must be a positive time value.")
      .createWithDefaultString("600s")

  val KUBERNETES_EXECUTOR_LOST_REASON_CHECK_MAX_ATTEMPTS =
    ConfigBuilder("spark.kubernetes.executor.lostCheck.maxAttempts")
      .doc("Maximum number of attempts allowed for checking the reason of an executor loss " +
        "before it is assumed that the executor failed.")
      .version("2.3.0")
      .intConf
      .checkValue(value => value > 0, "Maximum attempts of checks of executor lost reason " +
        "must be a positive integer")
      .createWithDefault(10)

  val WAIT_FOR_APP_COMPLETION =
    ConfigBuilder("spark.kubernetes.submission.waitAppCompletion")
      .doc("In cluster mode, whether to wait for the application to finish before exiting the " +
        "launcher process.")
      .version("2.3.0")
      .booleanConf
      .createWithDefault(true)

  val REPORT_INTERVAL =
    ConfigBuilder("spark.kubernetes.report.interval")
      .doc("Interval between reports of the current app status in cluster mode.")
      .version("2.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(interval => interval > 0, s"Logging interval must be a positive time value.")
      .createWithDefaultString("1s")

  val KUBERNETES_EXECUTOR_API_POLLING_INTERVAL =
    ConfigBuilder("spark.kubernetes.executor.apiPollingInterval")
      .doc("Interval between polls against the Kubernetes API server to inspect the " +
        "state of executors.")
      .version("2.4.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(interval => interval > 0, s"API server polling interval must be a" +
        " positive time value.")
      .createWithDefaultString("30s")

  val KUBERNETES_EXECUTOR_API_POLLING_WITH_RESOURCE_VERSION =
    ConfigBuilder("spark.kubernetes.executor.enablePollingWithResourceVersion")
      .doc("If true, `resourceVersion` is set with `0` during invoking pod listing APIs " +
        "in order to allow API Server-side caching. This should be used carefully.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val KUBERNETES_EXECUTOR_EVENT_PROCESSING_INTERVAL =
    ConfigBuilder("spark.kubernetes.executor.eventProcessingInterval")
      .doc("Interval between successive inspection of executor events sent from the" +
        " Kubernetes API.")
      .version("2.4.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(interval => interval > 0, s"Event processing interval must be a positive" +
        " time value.")
      .createWithDefaultString("1s")

  val MEMORY_OVERHEAD_FACTOR =
    ConfigBuilder("spark.kubernetes.memoryOverheadFactor")
      .doc("This sets the Memory Overhead Factor that will allocate memory to non-JVM jobs " +
        "which in the case of JVM tasks will default to 0.10 and 0.40 for non-JVM jobs")
      .version("2.4.0")
      .doubleConf
      .checkValue(mem_overhead => mem_overhead >= 0 && mem_overhead < 1,
        "Ensure that memory overhead is a double between 0 --> 1.0")
      .createWithDefault(0.1)

  val PYSPARK_MAJOR_PYTHON_VERSION =
    ConfigBuilder("spark.kubernetes.pyspark.pythonVersion")
      .doc(
        s"(Deprecated since Spark 3.1, please set '${PYSPARK_PYTHON.key}' and " +
        s"'${PYSPARK_DRIVER_PYTHON.key}' configurations or $ENV_PYSPARK_PYTHON and " +
        s"$ENV_PYSPARK_DRIVER_PYTHON environment variables instead.)")
      .version("2.4.0")
      .stringConf
      .checkValue("3" == _,
        "Python 2 was dropped from Spark 3.1, and only 3 is allowed in " +
          "this configuration. Note that this configuration was deprecated in Spark 3.1. " +
          s"Please set '${PYSPARK_PYTHON.key}' and '${PYSPARK_DRIVER_PYTHON.key}' " +
          s"configurations or $ENV_PYSPARK_PYTHON and $ENV_PYSPARK_DRIVER_PYTHON environment " +
          "variables instead.")
      .createOptional

  val KUBERNETES_KERBEROS_KRB5_FILE =
    ConfigBuilder("spark.kubernetes.kerberos.krb5.path")
      .doc("Specify the local location of the krb5.conf file to be mounted on the driver " +
        "and executors for Kerberos. Note: The KDC defined needs to be " +
        "visible from inside the containers ")
      .version("3.0.0")
      .stringConf
      .createOptional

  val KUBERNETES_KERBEROS_KRB5_CONFIG_MAP =
    ConfigBuilder("spark.kubernetes.kerberos.krb5.configMapName")
      .doc("Specify the name of the ConfigMap, containing the krb5.conf file, to be mounted " +
        "on the driver and executors for Kerberos. Note: The KDC defined" +
        "needs to be visible from inside the containers ")
      .version("3.0.0")
      .stringConf
      .createOptional

  val KUBERNETES_HADOOP_CONF_CONFIG_MAP =
    ConfigBuilder("spark.kubernetes.hadoop.configMapName")
      .doc("Specify the name of the ConfigMap, containing the HADOOP_CONF_DIR files, " +
        "to be mounted on the driver and executors for custom Hadoop configuration.")
      .version("3.0.0")
      .stringConf
      .createOptional

  val KUBERNETES_KERBEROS_DT_SECRET_NAME =
    ConfigBuilder("spark.kubernetes.kerberos.tokenSecret.name")
      .doc("Specify the name of the secret where your existing delegation tokens are stored. " +
        "This removes the need for the job user to provide any keytab for launching a job")
      .version("3.0.0")
      .stringConf
      .createOptional

  val KUBERNETES_KERBEROS_DT_SECRET_ITEM_KEY =
    ConfigBuilder("spark.kubernetes.kerberos.tokenSecret.itemKey")
      .doc("Specify the item key of the data where your existing delegation tokens are stored. " +
        "This removes the need for the job user to provide any keytab for launching a job")
      .version("3.0.0")
      .stringConf
      .createOptional

  val APP_RESOURCE_TYPE =
    ConfigBuilder("spark.kubernetes.resource.type")
      .internal()
      .doc("This sets the resource type internally")
      .version("2.4.1")
      .stringConf
      .checkValues(Set(APP_RESOURCE_TYPE_JAVA, APP_RESOURCE_TYPE_PYTHON, APP_RESOURCE_TYPE_R))
      .createOptional

  val KUBERNETES_LOCAL_DIRS_TMPFS =
    ConfigBuilder("spark.kubernetes.local.dirs.tmpfs")
      .doc("If set to true then emptyDir volumes created to back SPARK_LOCAL_DIRS will have " +
        "their medium set to Memory so that they will be created as tmpfs (i.e. RAM) backed " +
        "volumes. This may improve performance but scratch space usage will count towards " +
        "your pods memory limit so you may wish to request more memory.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  val KUBERNETES_DRIVER_PODTEMPLATE_FILE =
    ConfigBuilder("spark.kubernetes.driver.podTemplateFile")
      .doc("File containing a template pod spec for the driver")
      .version("3.0.0")
      .stringConf
      .createOptional

  val KUBERNETES_EXECUTOR_PODTEMPLATE_FILE =
    ConfigBuilder("spark.kubernetes.executor.podTemplateFile")
      .doc("File containing a template pod spec for executors")
      .version("3.0.0")
      .stringConf
      .createOptional

  val KUBERNETES_DRIVER_PODTEMPLATE_CONTAINER_NAME =
    ConfigBuilder("spark.kubernetes.driver.podTemplateContainerName")
      .doc("container name to be used as a basis for the driver in the given pod template")
      .version("3.0.0")
      .stringConf
      .createOptional

  val KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME =
    ConfigBuilder("spark.kubernetes.executor.podTemplateContainerName")
      .doc("container name to be used as a basis for executors in the given pod template")
      .version("3.0.0")
      .stringConf
      .createOptional

  val KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX =
    "spark.kubernetes.authenticate.submission"

  val KUBERNETES_TRUST_CERTIFICATES =
    ConfigBuilder("spark.kubernetes.trust.certificates")
      .doc("If set to true then client can submit to kubernetes cluster only with token")
      .version("3.2.0")
      .booleanConf
      .createWithDefault(false)

  val KUBERNETES_NODE_SELECTOR_PREFIX = "spark.kubernetes.node.selector."

  val KUBERNETES_DRIVER_NODE_SELECTOR_PREFIX = "spark.kubernetes.driver.node.selector."

  val KUBERNETES_EXECUTOR_NODE_SELECTOR_PREFIX = "spark.kubernetes.executor.node.selector."

  val KUBERNETES_DELETE_EXECUTORS =
    ConfigBuilder("spark.kubernetes.executor.deleteOnTermination")
      .doc("If set to false then executor pods will not be deleted in case " +
        "of failure or normal termination.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val KUBERNETES_DYN_ALLOC_KILL_GRACE_PERIOD =
    ConfigBuilder("spark.kubernetes.dynamicAllocation.deleteGracePeriod")
      .doc("How long to wait for executors to shut down gracefully before a forceful kill.")
      .version("3.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("5s")

  val KUBERNETES_SUBMIT_GRACE_PERIOD =
    ConfigBuilder("spark.kubernetes.appKillPodDeletionGracePeriod")
      .doc("Time to wait for graceful deletion of Spark pods when spark-submit" +
        " is used for killing an application.")
      .version("3.0.0")
      .timeConf(TimeUnit.SECONDS)
      .createOptional

  val KUBERNETES_FILE_UPLOAD_PATH =
    ConfigBuilder("spark.kubernetes.file.upload.path")
      .doc("Hadoop compatible file system path where files from the local file system " +
        "will be uploaded to in cluster mode.")
      .version("3.0.0")
      .stringConf
      .createOptional

  val KUBERNETES_EXECUTOR_CHECK_ALL_CONTAINERS =
    ConfigBuilder("spark.kubernetes.executor.checkAllContainers")
      .doc("If set to true, all containers in the executor pod will be checked when reporting" +
        "executor status.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  val KUBERNETES_EXECUTOR_MISSING_POD_DETECT_DELTA =
    ConfigBuilder("spark.kubernetes.executor.missingPodDetectDelta")
      .doc("When a registered executor's POD is missing from the Kubernetes API server's polled " +
        "list of PODs then this delta time is taken as the accepted time difference between the " +
        "registration time and the time of the polling. After this time the POD is considered " +
        "missing from the cluster and the executor will be removed.")
      .version("3.1.1")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(delay => delay > 0, "delay must be a positive time value")
      .createWithDefaultString("30s")

  val KUBERNETES_MAX_PENDING_PODS =
    ConfigBuilder("spark.kubernetes.allocation.maxPendingPods")
      .doc("Maximum number of pending PODs allowed during executor allocation for this " +
        "application. Those newly requested executors which are unknown by Kubernetes yet are " +
        "also counted into this limit as they will change into pending PODs by time. " +
        "This limit is independent from the resource profiles as it limits the sum of all " +
        "allocation for all the used resource profiles.")
      .version("3.2.0")
      .intConf
      .checkValue(value => value > 0, "Maximum number of pending pods should be a positive integer")
      .createWithDefault(Int.MaxValue)

  val KUBERNETES_DRIVER_LABEL_PREFIX = "spark.kubernetes.driver.label."
  val KUBERNETES_DRIVER_ANNOTATION_PREFIX = "spark.kubernetes.driver.annotation."
  val KUBERNETES_DRIVER_SERVICE_ANNOTATION_PREFIX = "spark.kubernetes.driver.service.annotation."
  val KUBERNETES_DRIVER_SECRETS_PREFIX = "spark.kubernetes.driver.secrets."
  val KUBERNETES_DRIVER_SECRET_KEY_REF_PREFIX = "spark.kubernetes.driver.secretKeyRef."
  val KUBERNETES_DRIVER_VOLUMES_PREFIX = "spark.kubernetes.driver.volumes."

  val KUBERNETES_EXECUTOR_LABEL_PREFIX = "spark.kubernetes.executor.label."
  val KUBERNETES_EXECUTOR_ANNOTATION_PREFIX = "spark.kubernetes.executor.annotation."
  val KUBERNETES_EXECUTOR_SECRETS_PREFIX = "spark.kubernetes.executor.secrets."
  val KUBERNETES_EXECUTOR_SECRET_KEY_REF_PREFIX = "spark.kubernetes.executor.secretKeyRef."
  val KUBERNETES_EXECUTOR_VOLUMES_PREFIX = "spark.kubernetes.executor.volumes."

  val KUBERNETES_VOLUMES_HOSTPATH_TYPE = "hostPath"
  val KUBERNETES_VOLUMES_PVC_TYPE = "persistentVolumeClaim"
  val KUBERNETES_VOLUMES_EMPTYDIR_TYPE = "emptyDir"
  val KUBERNETES_VOLUMES_NFS_TYPE = "nfs"
  val KUBERNETES_VOLUMES_MOUNT_PATH_KEY = "mount.path"
  val KUBERNETES_VOLUMES_MOUNT_SUBPATH_KEY = "mount.subPath"
  val KUBERNETES_VOLUMES_MOUNT_READONLY_KEY = "mount.readOnly"
  val KUBERNETES_VOLUMES_OPTIONS_PATH_KEY = "options.path"
  val KUBERNETES_VOLUMES_OPTIONS_CLAIM_NAME_KEY = "options.claimName"
  val KUBERNETES_VOLUMES_OPTIONS_CLAIM_STORAGE_CLASS_KEY = "options.storageClass"
  val KUBERNETES_VOLUMES_OPTIONS_MEDIUM_KEY = "options.medium"
  val KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY = "options.sizeLimit"
  val KUBERNETES_VOLUMES_OPTIONS_SERVER_KEY = "options.server"

  val KUBERNETES_DRIVER_ENV_PREFIX = "spark.kubernetes.driverEnv."

  val KUBERNETES_DNSNAME_MAX_LENGTH = 63
}
