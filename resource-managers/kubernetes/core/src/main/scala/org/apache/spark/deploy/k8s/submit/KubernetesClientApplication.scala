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

import java.util.concurrent.atomic.AtomicBoolean

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
import org.apache.spark.util.{ShutdownHookManager, Utils}

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

// SPARK-38079: thin, injectable wrapper around ShutdownHookManager's add/remove functions,
// used only by the cleanup-hook wiring in Client.run(). Bundled into one case class (rather
// than two separate constructor parameters on Client) so tests can override both together with
// a single fake, and so the real default (`ShutdownHookOps.default`) is defined in exactly one
// place. See the `shutdownHookOpsOverride` parameter on Client below for why this needs to be
// overridable at all: actually triggering a JVM shutdown hook from a test is impractical, so
// tests instead inject fakes here to assert Client.run() registers and removes a hook at the
// right times, without needing a real JVM shutdown.
private[submit] case class ShutdownHookOps(
    addHook: (() => Unit) => AnyRef,
    removeHook: AnyRef => Boolean)

private[submit] object ShutdownHookOps {
  def default: ShutdownHookOps =
    ShutdownHookOps(ShutdownHookManager.addShutdownHook, ShutdownHookManager.removeShutdownHook)
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
 * @param recoveryClientFactoryOverride overrides how the SPARK-38079 shutdown-hook cleanup path
 *                                      (see run()) builds the fresh Kubernetes client it uses to
 *                                      best-effort delete pre-resources left orphaned by an
 *                                      abrupt termination. Not reused from `kubernetesClient`
 *                                      because that one may already be closed, or concurrently
 *                                      in use, by the time the hook runs. Exposed only for
 *                                      testing; `None` (the default) builds a real client the
 *                                      same way KubernetesClientApplication.run() builds
 *                                      `kubernetesClient`. A plain default value can't itself
 *                                      call `conf.sparkConf` here, since `conf` is a
 *                                      constructor parameter, not yet a class member, at the
 *                                      point default values are evaluated.
 * @param shutdownHookOpsOverride overrides how the SPARK-38079 cleanup hook (see run()) is
 *                                 registered/removed. Exposed only for testing -- actually
 *                                 triggering a JVM shutdown hook from a test is impractical, so
 *                                 tests instead inject fakes here to assert that run() registers
 *                                 a hook before applying pre-resources and removes that exact
 *                                 hook once it is done with them (success or failure). `None`
 *                                 (the default) delegates to the real `ShutdownHookManager`.
 */
private[spark] class Client(
    conf: KubernetesDriverConf,
    builder: KubernetesDriverBuilder,
    kubernetesClient: KubernetesClient,
    watcher: LoggingPodStatusWatcher,
    recoveryClientFactoryOverride: Option[() => KubernetesClient] = None,
    shutdownHookOpsOverride: Option[ShutdownHookOps] = None) extends Logging {

  private val recoveryClientFactory: () => KubernetesClient = recoveryClientFactoryOverride
    .getOrElse(() => SparkKubernetesClientFactory.createKubernetesClient(
      KubernetesUtils.parseMasterUrl(conf.sparkConf.get("spark.master")),
      Some(conf.namespace),
      KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX,
      SparkKubernetesClientFactory.ClientType.Submission,
      conf.sparkConf,
      None))

  private val shutdownHookOps: ShutdownHookOps =
    shutdownHookOpsOverride.getOrElse(ShutdownHookOps.default)

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
    val resolvedDriverPod = new PodBuilder(resolvedDriverSpec.pod.pod)
      .editSpec()
        .addToContainers(resolvedDriverContainer)
        .addNewVolume()
          .withName(SPARK_CONF_VOLUME_DRIVER)
          .withNewConfigMap()
            .withItems(KubernetesClientUtils.buildKeyToPathObjects(confFilesMap).asJava)
            .withName(configMapName)
            .endConfigMap()
          .endVolume()
        .endSpec()
      .build()
    val driverPodName = resolvedDriverPod.getMetadata.getName

    // setup resources before pod creation
    // SPARK-38079: the driver's own base config map (mounted as SPARK_CONF_VOLUME_DRIVER
    // above) must also be created before the pod itself, to avoid a "configmap ... not
    // found" mount race between the driver pod and the config map it depends on.
    val preKubernetesResources = resolvedDriverSpec.driverPreKubernetesResources ++ Seq(configMap)

    // SPARK-38079: some of the pre-resources above (e.g. the Kerberos keytab/delegation token
    // secrets, the driver Kubernetes credentials secret) carry credentials. They are created
    // here without an owner reference -- the owner reference is only added once the driver pod
    // exists (see "Refresh all pre-resources' owner references" below), since Kubernetes owner
    // references require the owner's UID, which does not exist before the pod is created. If
    // this process is terminated abruptly in that window (e.g. Ctrl-C, SIGTERM, or a fatal JVM
    // error), the pre-resources would otherwise be silently orphaned in the namespace forever,
    // since Kubernetes only garbage-collects via owner references. This shutdown hook makes a
    // best-effort attempt to delete them (and the driver pod, if this submission created it) in
    // that case. It is a no-op -- and removed entirely, see the `finally` below -- once the
    // owner-reference refresh has completed; from that point on the existing owner references
    // make normal Kubernetes garbage collection sufficient, and e.g. Ctrl-C while waiting for
    // the application to complete must keep today's behavior of just detaching.
    val preResourcesApplied = new AtomicBoolean(false)
    val podCreatedByUs = new AtomicBoolean(false)
    val cleanupHookRef = shutdownHookOps.addHook(() => cleanupOrphanedPreResources(
      preKubernetesResources, driverPodName, preResourcesApplied.get(), podCreatedByUs.get()))

    var watch: Watch = null
    var createdDriverPod: Pod = null
    try {
      try {
        kubernetesClient.resourceList(preKubernetesResources: _*).forceConflicts().serverSideApply()
        preResourcesApplied.set(true)
      } catch {
        case NonFatal(e) =>
          logError("Please check \"kubectl auth can-i create [resource]\" first." +
            " It should be yes. And please also check your feature step implementation.")
          kubernetesClient.resourceList(preKubernetesResources: _*).delete()
          throw e
      }

      try {
        createdDriverPod =
          kubernetesClient.pods().inNamespace(conf.namespace).resource(resolvedDriverPod).create()
        podCreatedByUs.set(true)
      } catch {
        case NonFatal(e) =>
          kubernetesClient.resourceList(preKubernetesResources: _*).delete()
          logError("Please check \"kubectl auth can-i create pod\" first. It should be yes.")
          throw e
      }

      // Refresh all pre-resources' owner references
      try {
        addOwnerReference(createdDriverPod, preKubernetesResources)
        kubernetesClient.resourceList(preKubernetesResources: _*).forceConflicts().serverSideApply()
      } catch {
        case NonFatal(e) =>
          kubernetesClient.pods().resource(createdDriverPod).delete()
          kubernetesClient.resourceList(preKubernetesResources: _*).delete()
          throw e
      }
    } finally {
      // Past this point, the pre-resources either have an owner reference (success) or have
      // already been explicitly deleted by one of the catch blocks above (failure) -- either
      // way, the shutdown hook has nothing left to do, so remove it instead of leaving a no-op
      // hook registered for the remaining lifetime of this process.
      shutdownHookOps.removeHook(cleanupHookRef)
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

  // SPARK-38079: best-effort cleanup for pre-resources (and the driver pod, if it was created)
  // that were left without an owner reference because this process was terminated abruptly
  // before the owner-reference refresh in run() completed. See the comment above the shutdown
  // hook registration in run() for why this window exists and why it must stop mattering once
  // that refresh completes.
  //
  // package-private (rather than private) so it can be unit-tested directly -- actually
  // triggering a JVM shutdown hook from a test is impractical, so tests instead call this
  // method directly with injected pre/post-state and a fake recoveryClientFactory.
  private[submit] def cleanupOrphanedPreResources(
      preResources: Seq[HasMetadata],
      driverPodName: String,
      preResourcesApplied: Boolean,
      podCreatedByUs: Boolean): Unit = {
    if (preResourcesApplied) {
      try {
        Utils.tryWithResource(recoveryClientFactory()) { recoveryClient =>
          try {
            recoveryClient.resourceList(preResources: _*).delete()
            // Deliberately logged even on the success path (unlike the rest of this class,
            // which only logs failures): this runs during shutdown, so it is otherwise the
            // only signal an operator has that pre-resources were left behind by an abrupt
            // termination and had to be cleaned up here, rather than via the normal
            // owner-reference-based garbage collection.
            logInfo("Cleaned up orphaned pre-resources left behind by an abrupt shutdown.")
          } catch {
            case NonFatal(e) =>
              logWarning("Failed to clean up orphaned pre-resources on shutdown.", e)
          }
          if (podCreatedByUs) {
            try {
              recoveryClient.pods().inNamespace(conf.namespace).withName(driverPodName).delete()
              logInfo("Cleaned up the orphaned driver pod left behind by an abrupt shutdown.")
            } catch {
              case NonFatal(e) =>
                logWarning("Failed to clean up the orphaned driver pod on shutdown.", e)
            }
          }
        }
      } catch {
        case NonFatal(e) =>
          // Best-effort only: e.g. building the recovery client itself failed. There is
          // nothing more we can do here.
          logWarning("Failed to clean up orphaned pre-resources on shutdown.", e)
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
