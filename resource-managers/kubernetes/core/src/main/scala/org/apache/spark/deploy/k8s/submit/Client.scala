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

import java.util.{Collections, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesClientFactory
import org.apache.spark.deploy.k8s.submit.steps.DriverConfigurationStep
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Encapsulates arguments to the submission client.
 *
 * @param mainAppResource the main application resource
 * @param mainClass the main class of the application to run
 * @param driverArgs arguments to the driver
 */
private[spark] case class ClientArguments(
     mainAppResource: MainAppResource,
     mainClass: String,
     driverArgs: Array[String])

private[spark] object ClientArguments {

  def fromCommandLineArgs(args: Array[String]): ClientArguments = {
    var mainAppResource: Option[MainAppResource] = None
    var mainClass: Option[String] = None
    val driverArgs = mutable.ArrayBuffer.empty[String]

    args.sliding(2, 2).toList.foreach {
      case Array("--primary-java-resource", primaryJavaResource: String) =>
        mainAppResource = Some(JavaMainAppResource(primaryJavaResource))
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
      mainAppResource.get,
      mainClass.get,
      driverArgs.toArray)
  }
}

/**
 * Submits a Spark application to run on Kubernetes by creating the driver pod and starting a
 * watcher that monitors and logs the application status. Waits for the application to terminate if
 * spark.kubernetes.submission.waitAppCompletion is true.
 *
 * @param submissionSteps steps that collectively configure the driver
 * @param submissionSparkConf the submission client Spark configuration
 * @param kubernetesClient the client to talk to the Kubernetes API server
 * @param waitForAppCompletion a flag indicating whether the client should wait for the application
 *                             to complete
 * @param appName the application name
 * @param loggingPodStatusWatcher a watcher that monitors and logs the application status
 */
private[spark] class Client(
    submissionSteps: Seq[DriverConfigurationStep],
    submissionSparkConf: SparkConf,
    kubernetesClient: KubernetesClient,
    waitForAppCompletion: Boolean,
    appName: String,
    loggingPodStatusWatcher: LoggingPodStatusWatcher) extends Logging {

  private val driverJavaOptions = submissionSparkConf.get(
    org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS)

   /**
    * Run command that initializes a DriverSpec that will be updated after each
    * DriverConfigurationStep in the sequence that is passed in. The final KubernetesDriverSpec
    * will be used to build the Driver Container, Driver Pod, and Kubernetes Resources
    */
  def run(): Unit = {
    var currentDriverSpec = KubernetesDriverSpec.initialSpec(submissionSparkConf)
    // submissionSteps contain steps necessary to take, to resolve varying
    // client arguments that are passed in, created by orchestrator
    for (nextStep <- submissionSteps) {
      currentDriverSpec = nextStep.configureDriver(currentDriverSpec)
    }

    val resolvedDriverJavaOpts = currentDriverSpec
      .driverSparkConf
      // Remove this as the options are instead extracted and set individually below using
      // environment variables with prefix SPARK_JAVA_OPT_.
      .remove(org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS)
      .getAll
      .map {
        case (confKey, confValue) => s"-D$confKey=$confValue"
      } ++ driverJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val driverJavaOptsEnvs: Seq[EnvVar] = resolvedDriverJavaOpts.zipWithIndex.map {
      case (option, index) =>
        new EnvVarBuilder()
          .withName(s"$ENV_JAVA_OPT_PREFIX$index")
          .withValue(option)
          .build()
    }

    val resolvedDriverContainer = new ContainerBuilder(currentDriverSpec.driverContainer)
      .addAllToEnv(driverJavaOptsEnvs.asJava)
      .build()
    val resolvedDriverPod = new PodBuilder(currentDriverSpec.driverPod)
      .editSpec()
        .addToContainers(resolvedDriverContainer)
        .endSpec()
      .build()

    Utils.tryWithResource(
        kubernetesClient
          .pods()
          .withName(resolvedDriverPod.getMetadata.getName)
          .watch(loggingPodStatusWatcher)) { _ =>
      val createdDriverPod = kubernetesClient.pods().create(resolvedDriverPod)
      try {
        if (currentDriverSpec.otherKubernetesResources.nonEmpty) {
          val otherKubernetesResources = currentDriverSpec.otherKubernetesResources
          addDriverOwnerReference(createdDriverPod, otherKubernetesResources)
          kubernetesClient.resourceList(otherKubernetesResources: _*).createOrReplace()
        }
      } catch {
        case NonFatal(e) =>
          kubernetesClient.pods().delete(createdDriverPod)
          throw e
      }

      if (waitForAppCompletion) {
        logInfo(s"Waiting for application $appName to finish...")
        loggingPodStatusWatcher.awaitCompletion()
        logInfo(s"Application $appName finished.")
      } else {
        logInfo(s"Deployed Spark application $appName into Kubernetes.")
      }
    }
  }

  // Add a OwnerReference to the given resources making the driver pod an owner of them so when
  // the driver pod is deleted, the resources are garbage collected.
  private def addDriverOwnerReference(driverPod: Pod, resources: Seq[HasMetadata]): Unit = {
    val driverPodOwnerReference = new OwnerReferenceBuilder()
      .withName(driverPod.getMetadata.getName)
      .withApiVersion(driverPod.getApiVersion)
      .withUid(driverPod.getMetadata.getUid)
      .withKind(driverPod.getKind)
      .withController(true)
      .build()
    resources.foreach { resource =>
      val originalMetadata = resource.getMetadata
      originalMetadata.setOwnerReferences(Collections.singletonList(driverPodOwnerReference))
    }
  }
}

/**
 * Main class and entry point of application submission in KUBERNETES mode.
 */
private[spark] object Client extends SparkApplication {

  override def start(args: Array[String], conf: SparkConf): Unit = {
    val parsedArguments = ClientArguments.fromCommandLineArgs(args)
    run(parsedArguments, conf)
  }

  private def run(clientArguments: ClientArguments, sparkConf: SparkConf): Unit = {
    val namespace = sparkConf.get(KUBERNETES_NAMESPACE)
    val kubernetesAppId = s"spark-${UUID.randomUUID().toString.replaceAll("-", "")}"
    val launchTime = System.currentTimeMillis()
    val waitForAppCompletion = sparkConf.get(WAIT_FOR_APP_COMPLETION)
    val appName = sparkConf.getOption("spark.app.name").getOrElse("spark")
    val master = getK8sMasterUrl(sparkConf.get("spark.master"))
    val loggingInterval = Option(sparkConf.get(REPORT_INTERVAL)).filter(_ => waitForAppCompletion)

    val loggingPodStatusWatcher = new LoggingPodStatusWatcherImpl(
      kubernetesAppId, loggingInterval)

    val configurationStepsOrchestrator = new DriverConfigurationStepsOrchestrator(
      namespace,
      kubernetesAppId,
      launchTime,
      clientArguments.mainAppResource,
      appName,
      clientArguments.mainClass,
      clientArguments.driverArgs,
      sparkConf)

    Utils.tryWithResource(KubernetesClientFactory.createKubernetesClient(
      master,
      Some(namespace),
      KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX,
      sparkConf,
      None,
      None)) { kubernetesClient =>
        val client = new Client(
          configurationStepsOrchestrator.getAllConfigurationSteps(),
          sparkConf,
          kubernetesClient,
          waitForAppCompletion,
          appName,
          loggingPodStatusWatcher)
        client.run()
    }
  }
}
