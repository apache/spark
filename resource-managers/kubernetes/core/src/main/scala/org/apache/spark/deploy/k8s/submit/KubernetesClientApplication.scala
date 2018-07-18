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
package org.apache.spark.deploy.k8s.submit

import java.io.StringWriter
import java.util.{Collections, UUID}
import java.util.Properties

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpecificConf, SparkKubernetesClientFactory}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Encapsulates arguments to the submission client.
 *
 * @param mainAppResource the main application resource if any
 * @param mainClass the main class of the application to run
 * @param driverArgs arguments to the driver
 * @param maybePyFiles additional Python files via --py-files
 */
private[spark] case class ClientArguments(
    mainAppResource: Option[MainAppResource],
    mainClass: String,
    driverArgs: Array[String],
    maybePyFiles: Option[String])

private[spark] object ClientArguments {

  def fromCommandLineArgs(args: Array[String]): ClientArguments = {
    var mainAppResource: Option[MainAppResource] = None
    var mainClass: Option[String] = None
    val driverArgs = mutable.ArrayBuffer.empty[String]
    var maybePyFiles : Option[String] = None

    args.sliding(2, 2).toList.foreach {
      case Array("--primary-java-resource", primaryJavaResource: String) =>
        mainAppResource = Some(JavaMainAppResource(primaryJavaResource))
      case Array("--primary-py-file", primaryPythonResource: String) =>
        mainAppResource = Some(PythonMainAppResource(primaryPythonResource))
      case Array("--other-py-files", pyFiles: String) =>
        maybePyFiles = Some(pyFiles)
      case Array("--main-class", clazz: String) =>
        mainClass = Some(clazz)
      case Array("--arg", arg: String) =>
        driverArgs += arg
      case other =>
        val invalid = other.mkString(" ")
        throw new RuntimeException(s"Unknown arguments: $invalid")
    }

    require(mainClass.isDefined, "Main class must be specified via --main-class")

    ClientArguments(
      mainAppResource,
      mainClass.get,
      driverArgs.toArray,
      maybePyFiles)
  }
}

/**
 * Submits a Spark application to run on Kubernetes by creating the driver job and starting a
 * watcher that monitors and logs the application status. Waits for the application to terminate if
 * spark.kubernetes.submission.waitAppCompletion is true.
 *
 * @param builder Responsible for building the base driver job based on a composition of
 *                implemented features.
 * @param kubernetesConf application configuration
 * @param kubernetesClient the client to talk to the Kubernetes API server
 * @param waitForAppCompletion a flag indicating whether the client should wait for the application
 *                             to complete
 * @param appName the application name
 * @param watcher a watcher that monitors and logs the application status
 */
private[spark] class Client(
                             builder: KubernetesDriverBuilder,
                             kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf],
                             kubernetesClient: KubernetesClient,
                             waitForAppCompletion: Boolean,
                             appName: String,
                             watcher: LoggingJobStatusWatcher,
                             kubernetesResourceNamePrefix: String) extends Logging {

  def run(): Unit = {
    val resolvedDriverSpec = builder.buildFromFeatures(kubernetesConf)
    val configMapName = s"$kubernetesResourceNamePrefix-driver-conf-map"
    val configMap = buildConfigMap(configMapName, resolvedDriverSpec.systemProperties)
    // The include of the ENV_VAR for "SPARK_CONF_DIR" is to allow for the
    // Spark command builder to pickup on the Java Options present in the ConfigMap
    val resolvedDriverContainer = new ContainerBuilder(resolvedDriverSpec.job.container)
      .addNewEnv()
        .withName(ENV_SPARK_CONF_DIR)
        .withValue(SPARK_CONF_DIR_INTERNAL)
        .endEnv()
      .addNewVolumeMount()
        .withName(SPARK_CONF_VOLUME)
        .withMountPath(SPARK_CONF_DIR_INTERNAL)
        .endVolumeMount()
      .build()
    val resolvedDriverJob = new JobBuilder(resolvedDriverSpec.job.job)
      .editOrNewSpec()
        .editOrNewTemplate()
          .editOrNewSpec()
            .addToContainers(resolvedDriverContainer)
            .addNewVolume()
              .withName(SPARK_CONF_VOLUME)
              .withNewConfigMap()
                .withName(configMapName)
                .endConfigMap()
              .endVolume()
            .withRestartPolicy("OnFailure")
          .endSpec()
        .endTemplate()
      .endSpec()
      .build()
    // If the fabric8 kubernetes client will support kubernetes 1.8 this
    // should be removed and fixed with a more proper way
    // (https://github.com/fabric8io/kubernetes-client/issues/1020)
    resolvedDriverJob.getSpec.setAdditionalProperty("backoffLimit",
      kubernetesConf.get(KUBERNETES_DRIVER_JOB_BACKOFFLIMIT))
    Utils.tryWithResource(
      kubernetesClient.extensions()
        .jobs()
        .withName(resolvedDriverJob.getMetadata.getName)
        .watch(watcher)) { _ =>
      val createdDriverJob = kubernetesClient.extensions().jobs().create(resolvedDriverJob)
      try {
        val otherKubernetesResources =
          resolvedDriverSpec.driverKubernetesResources ++ Seq(configMap)
        addDriverOwnerReference(createdDriverJob, otherKubernetesResources)
        kubernetesClient.resourceList(otherKubernetesResources: _*).createOrReplace()
      } catch {
        case NonFatal(e) =>
          kubernetesClient.extensions().jobs().delete(createdDriverJob)
          throw e
      }

      if (waitForAppCompletion) {
        logInfo(s"Waiting for application $appName to finish...")
        watcher.awaitCompletion()
        logInfo(s"Application $appName finished.")
      } else {
        logInfo(s"Deployed Spark application $appName into Kubernetes.")
      }
    }
  }

  // Add a OwnerReference to the given resources making the driver job an owner of them so when
  // the driver job is deleted, the resources are garbage collected.
  private def addDriverOwnerReference(driverJob: Job, resources: Seq[HasMetadata]): Unit = {
    val driverJobOwnerReference = new OwnerReferenceBuilder()
      .withName(driverJob.getMetadata.getName)
      .withApiVersion(driverJob.getApiVersion)
      .withUid(driverJob.getMetadata.getUid)
      .withKind(driverJob.getKind)
      .withController(true)
      .build()
    resources.foreach { resource =>
      val originalMetadata = resource.getMetadata
      originalMetadata.setOwnerReferences(Collections.singletonList(driverJobOwnerReference))
    }
  }

  // Build a Config Map that will house spark conf properties in a single file for spark-submit
  private def buildConfigMap(configMapName: String, conf: Map[String, String]): ConfigMap = {
    val properties = new Properties()
    conf.foreach { case (k, v) =>
      properties.setProperty(k, v)
    }
    val propertiesWriter = new StringWriter()
    properties.store(propertiesWriter,
      s"Java properties built from Kubernetes config map with name: $configMapName")
    new ConfigMapBuilder()
      .withNewMetadata()
        .withName(configMapName)
        .endMetadata()
      .addToData(SPARK_CONF_FILE_NAME, propertiesWriter.toString)
      .build()
  }
}

/**
 * Main class and entry point of application submission in KUBERNETES mode.
 */
private[spark] class KubernetesClientApplication extends SparkApplication {

  override def start(args: Array[String], conf: SparkConf): Unit = {
    val parsedArguments = ClientArguments.fromCommandLineArgs(args)
    run(parsedArguments, conf)
  }

  private def run(clientArguments: ClientArguments, sparkConf: SparkConf): Unit = {
    val appName = sparkConf.getOption("spark.app.name").getOrElse("spark")
    // For constructing the app ID, we can't use the Spark application name, as the app ID is going
    // to be added as a label to group resources belonging to the same application. Label values are
    // considerably restrictive, e.g. must be no longer than 63 characters in length. So we generate
    // a unique app ID (captured by spark.app.id) in the format below.
    val kubernetesAppId = s"spark-${UUID.randomUUID().toString.replaceAll("-", "")}"
    val launchTime = System.currentTimeMillis()
    val waitForAppCompletion = sparkConf.get(WAIT_FOR_APP_COMPLETION)
    val kubernetesResourceNamePrefix = {
      s"$appName-$launchTime".toLowerCase.replaceAll("\\.", "-")
    }
    sparkConf.set(KUBERNETES_PYSPARK_PY_FILES, clientArguments.maybePyFiles.getOrElse(""))
    val kubernetesConf = KubernetesConf.createDriverConf(
      sparkConf,
      appName,
      kubernetesResourceNamePrefix,
      kubernetesAppId,
      clientArguments.mainAppResource,
      clientArguments.mainClass,
      clientArguments.driverArgs,
      clientArguments.maybePyFiles)
    val builder = new KubernetesDriverBuilder
    val namespace = kubernetesConf.namespace()
    // The master URL has been checked for validity already in SparkSubmit.
    // We just need to get rid of the "k8s://" prefix here.
    val master = sparkConf.get("spark.master").substring("k8s://".length)

    Utils.tryWithResource(SparkKubernetesClientFactory.createKubernetesClient(
      master,
      Some(namespace),
      KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX,
      sparkConf,
      None,
      None)) { kubernetesClient =>
        val client = new Client(
          builder,
          kubernetesConf,
          kubernetesClient,
          waitForAppCompletion,
          appName,
          new LoggingJobStatusWatcherImpl(kubernetesAppId, kubernetesClient),
          kubernetesResourceNamePrefix)
        client.run()
    }
  }
}
