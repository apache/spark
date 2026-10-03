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

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks._
import scala.util.control.NonFatal

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesUtils.addOwnerReference
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{APP_ID, APP_NAME, SUBMISSION_ID}
import org.apache.spark.util.Utils

/**
 * Encapsulates arguments to the submission client.
 *
 * @param mainAppResource the main application resource if any
 * @param mainClass the main class of the application to run
 * @param driverArgs arguments to the driver
 */
private[spark] case class ClientArguments(
    mainAppResource: MainAppResource,
    mainClass: String,
    driverArgs: Array[String],
    proxyUser: Option[String])

private[spark] object ClientArguments {

  def fromCommandLineArgs(args: Array[String]): ClientArguments = {
    var mainAppResource: MainAppResource = JavaMainAppResource(None)
    var mainClass: Option[String] = None
    val driverArgs = mutable.ArrayBuffer.empty[String]
    var proxyUser: Option[String] = None

    args.sliding(2, 2).toList.foreach {
      case Array("--primary-java-resource", primaryJavaResource: String) =>
        mainAppResource = JavaMainAppResource(Some(primaryJavaResource))
      case Array("--primary-py-file", primaryPythonResource: String) =>
        mainAppResource = PythonMainAppResource(primaryPythonResource)
      case Array("--primary-r-file", primaryRFile: String) =>
        mainAppResource = RMainAppResource(primaryRFile)
      case Array("--main-class", clazz: String) =>
        mainClass = Some(clazz)
      case Array("--arg", arg: String) =>
        driverArgs += arg
      case Array("--proxy-user", user: String) =>
        proxyUser = Some(user)
      case other =>
        val invalid = other.mkString(" ")
        throw new RuntimeException(s"Unknown arguments: $invalid")
    }

    require(mainClass.isDefined, "Main class must be specified via --main-class")

    ClientArguments(
      mainAppResource,
      mainClass.get,
      driverArgs.toArray,
      proxyUser)
  }
}

/**
 * Submits a Spark application to run on Kubernetes by creating the driver pod and starting a
 * watcher that monitors and logs the application status. Waits for the application to terminate if
 * spark.kubernetes.submission.waitAppCompletion is true.
 *
 * @param conf The kubernetes driver config.
 * @param builder Responsible for building the base driver pod based on a composition of
 *                implemented features.
 * @param kubernetesClient the client to talk to the Kubernetes API server
 * @param watcher a watcher that monitors and logs the application status
 */
private[spark] class Client(
    conf: KubernetesDriverConf,
    builder: KubernetesDriverBuilder,
    kubernetesClient: KubernetesClient,
    watcher: LoggingPodStatusWatcher) extends Logging {

  def run(): Unit = {
    val resolvedDriverSpec = builder.buildFromFeatures(conf, kubernetesClient)
    val configMapName = KubernetesClientUtils.configMapNameDriver
    val confFilesMap = KubernetesClientUtils.buildSparkConfDirFilesMap(configMapName,
      conf.sparkConf, resolvedDriverSpec.systemProperties)
    val configMap = KubernetesClientUtils.buildConfigMap(configMapName, confFilesMap +
        (KUBERNETES_NAMESPACE.key -> conf.namespace))

    // The include of the ENV_VAR for "SPARK_CONF_DIR" is to allow for the
    // Spark command builder to pickup on the Java Options present in the ConfigMap
    val resolvedDriverContainer = new ContainerBuilder(resolvedDriverSpec.pod.container)
      .addNewEnv()
        .withName(ENV_SPARK_CONF_DIR)
        .withValue(SPARK_CONF_DIR_INTERNAL)
        .endEnv()
      .addNewVolumeMount()
        .withName(SPARK_CONF_VOLUME_DRIVER)
        .withMountPath(SPARK_CONF_DIR_INTERNAL)
        .endVolumeMount()
      .build()
    // SPARK-38079: a pod template (spark.kubernetes.driver.podTemplateFile) may already pin
    // spec.nodeName to a specific node. The Kubernetes API rejects creating a pod that has
    // both a non-empty spec.nodeName and a non-empty spec.schedulingGates (added below) --
    // "nodeName cannot be set until all schedulingGates have been cleared" -- since nodeName
    // is meant to be set only once nothing (including scheduling) stands between the pod and
    // that node. Translate it to an equivalent kubernetes.io/hostname nodeSelector entry
    // instead, which (unlike nodeName) is created and read normally on a gated pod and has
    // the same effect once the scheduler runs -- unless spark.kubernetes.driver.node.selector.
    // kubernetes.io/hostname (via BasicDriverFeatureStep, applied earlier in
    // builder.buildFromFeatures() above) already set that same key: that explicit user
    // configuration always wins, so it is never overwritten by this fallback.
    val pinnedNodeName = Option(resolvedDriverSpec.pod.pod.getSpec.getNodeName)
      .filter(_.nonEmpty)
      .filter(_ => !Option(resolvedDriverSpec.pod.pod.getSpec.getNodeSelector)
        .exists(_.containsKey("kubernetes.io/hostname")))
    val resolvedDriverPod = new PodBuilder(resolvedDriverSpec.pod.pod)
      .editSpec()
        .withNodeName(null)
        .addToContainers(resolvedDriverContainer)
        .addNewVolume()
          .withName(SPARK_CONF_VOLUME_DRIVER)
          .withNewConfigMap()
            .withItems(KubernetesClientUtils.buildKeyToPathObjects(confFilesMap).asJava)
            .withName(configMapName)
            .endConfigMap()
          .endVolume()
        .addToNodeSelector(
          pinnedNodeName.map(nodeName => Map("kubernetes.io/hostname" -> nodeName))
            .getOrElse(Map.empty[String, String]).asJava)
        // SPARK-38079: holds the pod unschedulable -- so kubelet cannot attempt to mount
        // anything on it -- until its pre-resources exist (see below). Only removed once
        // that is true, right before the watch loop; see the "Remove the pre-resources
        // scheduling gate" step below for why this makes an ownerless-resource window,
        // and its consequences (a shutdown hook), unnecessary.
        .addNewSchedulingGate(PRE_RESOURCES_SCHEDULING_GATE)
        .endSpec()
      .build()
    val driverPodName = resolvedDriverPod.getMetadata.getName

    // SPARK-38079: the driver's own base config map (mounted as SPARK_CONF_VOLUME_DRIVER
    // above) must also be created before the pod is schedulable, to avoid a "configmap ...
    // not found" mount race between the driver pod and the config map it depends on.
    val preKubernetesResources = resolvedDriverSpec.driverPreKubernetesResources ++ Seq(configMap)

    var watch: Watch = null
    val createdDriverPod: Pod =
      try {
        kubernetesClient.pods().inNamespace(conf.namespace).resource(resolvedDriverPod).create()
      } catch {
        case NonFatal(e) =>
          logError("Please check \"kubectl auth can-i create pod\" first. It should be yes.")
          throw e
      }

    // SPARK-38079: some of the pre-resources above (e.g. the Kerberos keytab/delegation token
    // secrets, the driver Kubernetes credentials secret) carry credentials. Now that the
    // driver pod exists, its UID is known, so each pre-resource's owner reference can be set
    // before -- and included in -- the single call that creates it. The pod itself is still
    // scheduling-gated, so nothing can be scheduled against it (and so nothing can attempt to
    // mount these resources) until they exist -- but they are never ownerless at any point
    // after this call: either it succeeds and every pre-resource already has an owner
    // reference, or it fails and none of them (that made it to the server) are left
    // referencing anything, since the driver pod that would have owned them is deleted in the
    // catch block below.
    try {
      addOwnerReference(createdDriverPod, preKubernetesResources)
      kubernetesClient.resourceList(preKubernetesResources: _*).forceConflicts().serverSideApply()
    } catch {
      case NonFatal(e) =>
        logError("Please check \"kubectl auth can-i create [resource]\" first." +
          " It should be yes. And please also check your feature step implementation.")
        deletePodAndPreResources(createdDriverPod, preKubernetesResources)
        throw e
    }

    // SPARK-38079: remove the pre-resources scheduling gate now that its pre-resources exist,
    // letting the scheduler proceed with this pod. If this process is terminated abruptly
    // before this point (e.g. Ctrl-C, SIGTERM, or a fatal JVM error), the driver pod is left
    // behind still gated -- inert (kubelet cannot schedule/mount anything on a gated pod) and
    // visible as `Pending`/`SchedulingGated` via any standard pod listing. That is a low-
    // severity leak, in the same class Spark already accepts elsewhere for a process killed
    // right after pod creation (e.g. Ctrl-C before the watch loop below starts), so no
    // shutdown hook is registered to guard this window.
    try {
      kubernetesClient.pods().inNamespace(conf.namespace).withName(driverPodName).edit(
        (currentPod: Pod) => new PodBuilder(currentPod)
          .editSpec()
            .removeMatchingFromSchedulingGates(_.getName == PRE_RESOURCES_SCHEDULING_GATE)
            .endSpec()
          .build())
    } catch {
      case NonFatal(e) =>
        logError("Please check \"kubectl auth can-i patch pod\" first. It should be yes.")
        deletePodAndPreResources(createdDriverPod, preKubernetesResources)
        throw e
    }

    // setup resources after pod creation, and refresh all resources' owner references
    try {
      val otherKubernetesResources = resolvedDriverSpec.driverKubernetesResources
      addOwnerReference(createdDriverPod, otherKubernetesResources)
      kubernetesClient.resourceList(otherKubernetesResources: _*).forceConflicts().serverSideApply()
    } catch {
      case NonFatal(e) =>
        kubernetesClient.pods().resource(createdDriverPod).delete()
        throw e
    }

    val sId = Client.submissionId(conf.namespace, driverPodName)
    if (conf.get(WAIT_FOR_APP_COMPLETION)) {
      breakable {
        while (true) {
          val podWithName = kubernetesClient
            .pods()
            .inNamespace(conf.namespace)
            .withName(driverPodName)
          // Reset resource to old before we start the watch, this is important for race conditions
          watcher.reset()
          watch = podWithName.watch(watcher)

          // Send the latest pod state we know to the watcher to make sure we didn't miss anything
          watcher.eventReceived(Action.MODIFIED, podWithName.get())

          // Break the while loop if the pod is completed or we don't want to wait
          if (watcher.watchOrStop(sId)) {
            watch.close()
            break()
          }
        }
      }
    } else {
      logInfo(log"Deployed Spark application ${MDC(APP_NAME, conf.appName)} with " +
        log"application ID ${MDC(APP_ID, conf.appId)} and " +
        log"submission ID ${MDC(SUBMISSION_ID, sId)} into Kubernetes")
    }
  }

  // SPARK-38079: best-effort cleanup for the two failure catch blocks between pod creation and
  // gate removal. The pod and each pre-resource are deleted independently -- every delete call
  // wrapped in its own Utils.tryLogNonFatalError -- so that one of them failing (e.g. a delete
  // call itself hitting a permission or network error) neither masks the original exception
  // the caller is about to (re)throw, nor skips the rest. In particular, fabric8's
  // resourceList(...).delete() deletes the given items sequentially and stops at the first
  // non-404 exception, so pre-resources are deleted one at a time here rather than as a single
  // resourceList(...).delete() call, which could otherwise leave every pre-resource after the
  // one that failed undeleted.
  private def deletePodAndPreResources(pod: Pod, preResources: Seq[HasMetadata]): Unit = {
    Utils.tryLogNonFatalError {
      kubernetesClient.pods().resource(pod).delete()
    }
    preResources.foreach { resource =>
      Utils.tryLogNonFatalError {
        kubernetesClient.resource(resource).delete()
      }
    }
  }
}

private[spark] object Client {
  def submissionId(namespace: String, driverPodName: String): String = s"$namespace:$driverPodName"
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
    // For constructing the app ID, we can't use the Spark application name, as the app ID is going
    // to be added as a label to group resources belonging to the same application. Label values are
    // considerably restrictive, e.g. must be no longer than 63 characters in length. So we generate
    // a unique app ID (captured by spark.app.id) in the format below.
    val kubernetesAppId = KubernetesConf.getKubernetesAppId()
    val kubernetesConf = KubernetesConf.createDriverConf(
      sparkConf,
      kubernetesAppId,
      clientArguments.mainAppResource,
      clientArguments.mainClass,
      clientArguments.driverArgs,
      clientArguments.proxyUser)
    // The master URL has been checked for validity already in SparkSubmit.
    // We just need to get rid of the "k8s://" prefix here.
    val master = KubernetesUtils.parseMasterUrl(sparkConf.get("spark.master"))
    val watcher = new LoggingPodStatusWatcherImpl(kubernetesConf)

    Utils.tryWithResource(SparkKubernetesClientFactory.createKubernetesClient(
      master,
      Some(kubernetesConf.namespace),
      KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX,
      SparkKubernetesClientFactory.ClientType.Submission,
      sparkConf,
      None)) { kubernetesClient =>
        val client = new Client(
          kubernetesConf,
          new KubernetesDriverBuilder(),
          kubernetesClient,
          watcher)
        client.run()
    }
  }
}
