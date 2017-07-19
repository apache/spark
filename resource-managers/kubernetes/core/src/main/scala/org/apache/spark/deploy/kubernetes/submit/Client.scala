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
package org.apache.spark.deploy.kubernetes.submit

import java.util.{Collections, UUID}

import io.fabric8.kubernetes.api.model.{ContainerBuilder, OwnerReferenceBuilder, PodBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.submitsteps.{DriverConfigurationStep, KubernetesDriverSpec}
import org.apache.spark.deploy.kubernetes.SparkKubernetesClientFactory
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[spark] case class ClientArguments(
     mainAppResource: MainAppResource,
     otherPyFiles: Seq[String],
     mainClass: String,
     driverArgs: Array[String])

private[spark] object ClientArguments {
  def fromCommandLineArgs(args: Array[String]): ClientArguments = {
    var mainAppResource: Option[MainAppResource] = None
    var otherPyFiles = Seq.empty[String]
    var mainClass: Option[String] = None
    val driverArgs = mutable.Buffer.empty[String]
    args.sliding(2, 2).toList.collect {
      case Array("--primary-py-file", mainPyFile: String) =>
        mainAppResource = Some(PythonMainAppResource(mainPyFile))
      case Array("--primary-java-resource", primaryJavaResource: String) =>
        mainAppResource = Some(JavaMainAppResource(primaryJavaResource))
      case Array("--main-class", clazz: String) =>
        mainClass = Some(clazz)
      case Array("--other-py-files", pyFiles: String) =>
        otherPyFiles = pyFiles.split(",")
      case Array("--arg", arg: String) =>
        driverArgs += arg
      case other =>
        val invalid = other.mkString(" ")
        throw new RuntimeException(s"Unknown arguments: $invalid")
    }
    require(mainAppResource.isDefined,
        "Main app resource must be defined by either --primary-py-file or --primary-java-resource.")
    require(mainClass.isDefined, "Main class must be specified via --main-class")
    ClientArguments(
        mainAppResource.get,
        otherPyFiles,
        mainClass.get,
        driverArgs.toArray)
  }
}

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
    * Run command that initalizes a DriverSpec that will be updated after each
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
      // We don't need this anymore since we just set the JVM options on the environment
      .remove(org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS)
      .getAll
      .map {
        case (confKey, confValue) => s"-D$confKey=$confValue"
      }.mkString(" ") + driverJavaOptions.map(" " + _).getOrElse("")
    val resolvedDriverContainer = new ContainerBuilder(currentDriverSpec.driverContainer)
      .addNewEnv()
        .withName(ENV_DRIVER_JAVA_OPTS)
        .withValue(resolvedDriverJavaOpts)
        .endEnv()
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
          val driverPodOwnerReference = new OwnerReferenceBuilder()
            .withName(createdDriverPod.getMetadata.getName)
            .withApiVersion(createdDriverPod.getApiVersion)
            .withUid(createdDriverPod.getMetadata.getUid)
            .withKind(createdDriverPod.getKind)
            .withController(true)
            .build()
          currentDriverSpec.otherKubernetesResources.foreach { resource =>
            val originalMetadata = resource.getMetadata
            originalMetadata.setOwnerReferences(Collections.singletonList(driverPodOwnerReference))
          }
          val otherKubernetesResources = currentDriverSpec.otherKubernetesResources
          kubernetesClient.resourceList(otherKubernetesResources: _*).createOrReplace()
        }
      } catch {
        case e: Throwable =>
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
}

private[spark] object Client {
  def run(sparkConf: SparkConf, clientArguments: ClientArguments): Unit = {
    val namespace = sparkConf.get(KUBERNETES_NAMESPACE)
    val kubernetesAppId = s"spark-${UUID.randomUUID().toString.replaceAll("-", "")}"
    val launchTime = System.currentTimeMillis()
    val waitForAppCompletion = sparkConf.get(WAIT_FOR_APP_COMPLETION)
    val appName = sparkConf.getOption("spark.app.name").getOrElse("spark")
    val master = resolveK8sMaster(sparkConf.get("spark.master"))
    val loggingInterval = Option(sparkConf.get(REPORT_INTERVAL)).filter( _ => waitForAppCompletion)
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
        clientArguments.otherPyFiles,
        sparkConf)
    Utils.tryWithResource(SparkKubernetesClientFactory.createKubernetesClient(
        master,
        Some(namespace),
        APISERVER_AUTH_SUBMISSION_CONF_PREFIX,
        sparkConf,
        None,
        None)) { kubernetesClient =>
      new Client(
          configurationStepsOrchestrator.getAllConfigurationSteps(),
          sparkConf,
          kubernetesClient,
          waitForAppCompletion,
          appName,
          loggingPodStatusWatcher).run()
    }
  }

   /**
    * Entry point from SparkSubmit in spark-core
    *
    * @param args Array of strings that have interchanging values that will be
    *             parsed by ClientArguments with the identifiers that precede the values
    */
  def main(args: Array[String]): Unit = {
    val parsedArguments = ClientArguments.fromCommandLineArgs(args)
    val sparkConf = new SparkConf()
    run(sparkConf, parsedArguments)
  }
}
