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

import java.io.{File, IOException}
import java.net.URI
import java.security.SecureRandom
import java.util.{Collections, UUID}

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{Container, ContainerBuilder, ContainerStateRunning, ContainerStateTerminated, ContainerStateWaiting, ContainerStatus, HasMetadata, OwnerReferenceBuilder, Pod, PodBuilder, Quantity}
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.annotation.{DeveloperApi, Since, Unstable}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.k8s.Config.KUBERNETES_FILE_UPLOAD_PATH
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.resource.ResourceUtils
import org.apache.spark.util.{Clock, SystemClock, Utils}
import org.apache.spark.util.DependencyUtils.downloadFile
import org.apache.spark.util.Utils.getHadoopFileSystem

/**
 * :: DeveloperApi ::
 *
 * A utility class used for K8s operations internally and for implementing ExternalClusterManagers.
 */
@Unstable
@DeveloperApi
object KubernetesUtils extends Logging {

  private val systemClock = new SystemClock()
  private lazy val RNG = new SecureRandom()

  /**
   * Extract and parse Spark configuration properties with a given name prefix and
   * return the result as a Map. Keys must not have more than one value.
   *
   * @param sparkConf Spark configuration
   * @param prefix the given property name prefix
   * @return a Map storing the configuration property keys and values
   */
  @Since("2.3.0")
  def parsePrefixedKeyValuePairs(
      sparkConf: SparkConf,
      prefix: String): Map[String, String] = {
    sparkConf.getAllWithPrefix(prefix).toMap
  }

  @Since("3.0.0")
  def requireBothOrNeitherDefined(
      opt1: Option[_],
      opt2: Option[_],
      errMessageWhenFirstIsMissing: String,
      errMessageWhenSecondIsMissing: String): Unit = {
    requireSecondIfFirstIsDefined(opt1, opt2, errMessageWhenSecondIsMissing)
    requireSecondIfFirstIsDefined(opt2, opt1, errMessageWhenFirstIsMissing)
  }

  @Since("3.0.0")
  def requireSecondIfFirstIsDefined(
      opt1: Option[_],
      opt2: Option[_],
      errMessageWhenSecondIsMissing: String): Unit = {
    opt1.foreach { _ =>
      require(opt2.isDefined, errMessageWhenSecondIsMissing)
    }
  }

  @Since("2.3.0")
  def requireNandDefined(opt1: Option[_], opt2: Option[_], errMessage: String): Unit = {
    opt1.foreach { _ => require(opt2.isEmpty, errMessage) }
    opt2.foreach { _ => require(opt1.isEmpty, errMessage) }
  }

  @Since("3.2.0")
  def loadPodFromTemplate(
      kubernetesClient: KubernetesClient,
      templateFileName: String,
      containerName: Option[String],
      conf: SparkConf): SparkPod = {
    try {
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
      val localFile = downloadFile(templateFileName, Utils.createTempDir(), conf, hadoopConf)
      val templateFile = new File(new java.net.URI(localFile).getPath)
      val pod = kubernetesClient.pods().load(templateFile).get()
      selectSparkContainer(pod, containerName)
    } catch {
      case e: Exception =>
        logError(
          s"Encountered exception while attempting to load initial pod spec from file", e)
        throw new SparkException("Could not load pod from template file.", e)
    }
  }

  @Since("3.0.0")
  def selectSparkContainer(pod: Pod, containerName: Option[String]): SparkPod = {
    def selectNamedContainer(
      containers: List[Container], name: String): Option[(Container, List[Container])] =
      containers.partition(_.getName == name) match {
        case (sparkContainer :: Nil, rest) => Some((sparkContainer, rest))
        case _ =>
          logWarning(
            s"specified container ${name} not found on pod template, " +
              s"falling back to taking the first container")
          Option.empty
      }
    val containers = pod.getSpec.getContainers.asScala.toList
    containerName
      .flatMap(selectNamedContainer(containers, _))
      .orElse(containers.headOption.map((_, containers.tail)))
      .map {
        case (sparkContainer: Container, rest: List[Container]) => SparkPod(
          new PodBuilder(pod)
            .editSpec()
            .withContainers(rest.asJava)
            .endSpec()
            .build(),
          sparkContainer)
      }.getOrElse(SparkPod(pod, new ContainerBuilder().build()))
  }

  @Since("2.4.0")
  def parseMasterUrl(url: String): String = url.substring("k8s://".length)

  @Since("3.0.0")
  def formatPairsBundle(pairs: Seq[(String, String)], indent: Int = 1) : String = {
    // Use more loggable format if value is null or empty
    val indentStr = "\t" * indent
    pairs.map {
      case (k, v) => s"\n$indentStr $k: ${Option(v).filter(_.nonEmpty).getOrElse("N/A")}"
    }.mkString("")
  }

  /**
   * Given a pod, output a human readable representation of its state
   *
   * @param pod Pod
   * @return Human readable pod state
   */
  @Since("3.0.0")
  def formatPodState(pod: Pod): String = {
    val details = Seq[(String, String)](
      // pod metadata
      ("pod name", pod.getMetadata.getName),
      ("namespace", pod.getMetadata.getNamespace),
      ("labels", pod.getMetadata.getLabels.asScala.mkString(", ")),
      ("pod uid", pod.getMetadata.getUid),
      ("creation time", formatTime(pod.getMetadata.getCreationTimestamp)),

      // spec details
      ("service account name", pod.getSpec.getServiceAccountName),
      ("volumes", pod.getSpec.getVolumes.asScala.map(_.getName).mkString(", ")),
      ("node name", pod.getSpec.getNodeName),

      // status
      ("start time", formatTime(pod.getStatus.getStartTime)),
      ("phase", pod.getStatus.getPhase),
      ("container status", containersDescription(pod, 2))
    )

    formatPairsBundle(details)
  }

  @Since("3.0.0")
  def containersDescription(p: Pod, indent: Int = 1): String = {
    p.getStatus.getContainerStatuses.asScala.map { status =>
      Seq(
        ("container name", status.getName),
        ("container image", status.getImage)) ++
        containerStatusDescription(status)
    }.map(p => formatPairsBundle(p, indent)).mkString("\n\n")
  }

  @Since("3.0.0")
  def containerStatusDescription(containerStatus: ContainerStatus)
    : Seq[(String, String)] = {
    val state = containerStatus.getState
    Option(state.getRunning)
      .orElse(Option(state.getTerminated))
      .orElse(Option(state.getWaiting))
      .map {
        case running: ContainerStateRunning =>
          Seq(
            ("container state", "running"),
            ("container started at", formatTime(running.getStartedAt)))
        case waiting: ContainerStateWaiting =>
          Seq(
            ("container state", "waiting"),
            ("pending reason", waiting.getReason))
        case terminated: ContainerStateTerminated =>
          Seq(
            ("container state", "terminated"),
            ("container started at", formatTime(terminated.getStartedAt)),
            ("container finished at", formatTime(terminated.getFinishedAt)),
            ("exit code", terminated.getExitCode.toString),
            ("termination reason", terminated.getReason))
        case unknown =>
          throw new SparkException(s"Unexpected container status type ${unknown.getClass}.")
      }.getOrElse(Seq(("container state", "N/A")))
  }

  @Since("3.0.0")
  def formatTime(time: String): String = {
    if (time != null) time else "N/A"
  }

  /**
   * Generates a unique ID to be used as part of identifiers. The returned ID is a hex string
   * of a 64-bit value containing the 40 LSBs from the current time + 24 random bits from a
   * cryptographically strong RNG. (40 bits gives about 30 years worth of "unique" timestamps.)
   *
   * This avoids using a UUID for uniqueness (too long), and relying solely on the current time
   * (not unique enough).
   */
  @Since("3.0.0")
  def uniqueID(clock: Clock = systemClock): String = {
    val random = new Array[Byte](3)
    synchronized {
      RNG.nextBytes(random)
    }

    val time = java.lang.Long.toHexString(clock.getTimeMillis() & 0xFFFFFFFFFFL)
    Hex.encodeHexString(random) + time
  }

  /**
   * This function builds the Quantity objects for each resource in the Spark resource
   * configs based on the component name(spark.driver.resource or spark.executor.resource).
   * It assumes we can use the Kubernetes device plugin format: vendor-domain/resource.
   * It returns a set with a tuple of vendor-domain/resource and Quantity for each resource.
   */
  @Since("3.0.0")
  def buildResourcesQuantities(
      componentName: String,
      sparkConf: SparkConf): Map[String, Quantity] = {
    val requests = ResourceUtils.parseAllResourceRequests(sparkConf, componentName)
    requests.map { request =>
      val vendorDomain = if (request.vendor.isPresent()) {
        request.vendor.get()
      } else {
        throw new SparkException(s"Resource: ${request.id.resourceName} was requested, " +
          "but vendor was not specified.")
      }
      val quantity = new Quantity(request.amount.toString)
      (KubernetesConf.buildKubernetesResourceName(vendorDomain, request.id.resourceName), quantity)
    }.toMap
  }

  /**
   * Upload files and modify their uris
   */
  @Since("3.0.0")
  def uploadAndTransformFileUris(fileUris: Iterable[String], conf: Option[SparkConf] = None)
    : Iterable[String] = {
    fileUris.map { uri =>
      uploadFileUri(uri, conf)
    }
  }

  private def isLocalDependency(uri: URI): Boolean = {
    uri.getScheme match {
      case null | "file" => true
      case _ => false
    }
  }

  @Since("3.0.0")
  def isLocalAndResolvable(resource: String): Boolean = {
    resource != SparkLauncher.NO_RESOURCE &&
      isLocalDependency(Utils.resolveURI(resource))
  }

  @Since("3.1.1")
  def renameMainAppResource(
      resource: String,
      conf: Option[SparkConf] = None,
      shouldUploadLocal: Boolean): String = {
    if (isLocalAndResolvable(resource)) {
      if (shouldUploadLocal) {
        uploadFileUri(resource, conf)
      } else {
        SparkLauncher.NO_RESOURCE
      }
    } else {
      resource
    }
  }

  @Since("3.0.0")
  def uploadFileUri(uri: String, conf: Option[SparkConf] = None): String = {
    conf match {
      case Some(sConf) =>
        if (sConf.get(KUBERNETES_FILE_UPLOAD_PATH).isDefined) {
          val fileUri = Utils.resolveURI(uri)
          try {
            val hadoopConf = SparkHadoopUtil.get.newConfiguration(sConf)
            val uploadPath = sConf.get(KUBERNETES_FILE_UPLOAD_PATH).get
            val fs = getHadoopFileSystem(Utils.resolveURI(uploadPath), hadoopConf)
            val randomDirName = s"spark-upload-${UUID.randomUUID()}"
            fs.mkdirs(new Path(s"${uploadPath}/${randomDirName}"))
            val targetUri = s"${uploadPath}/${randomDirName}/${fileUri.getPath.split("/").last}"
            log.info(s"Uploading file: ${fileUri.getPath} to dest: $targetUri...")
            uploadFileToHadoopCompatibleFS(new Path(fileUri.getPath), new Path(targetUri), fs)
            targetUri
          } catch {
            case e: Exception =>
              throw new SparkException(s"Uploading file ${fileUri.getPath} failed...", e)
          }
        } else {
          throw new SparkException("Please specify " +
            "spark.kubernetes.file.upload.path property.")
        }
      case _ => throw new SparkException("Spark configuration is missing...")
    }
  }

  /**
   * Upload a file to a Hadoop-compatible filesystem.
   */
  private def uploadFileToHadoopCompatibleFS(
      src: Path,
      dest: Path,
      fs: FileSystem,
      delSrc : Boolean = false,
      overwrite: Boolean = true): Unit = {
    try {
      fs.copyFromLocalFile(delSrc, overwrite, src, dest)
    } catch {
      case e: IOException =>
        throw new SparkException(s"Error uploading file ${src.getName}", e)
    }
  }

  @Since("3.0.0")
  def buildPodWithServiceAccount(serviceAccount: Option[String], pod: SparkPod): Option[Pod] = {
    serviceAccount.map { account =>
      new PodBuilder(pod.pod)
        .editOrNewSpec()
          .withServiceAccount(account)
          .withServiceAccountName(account)
        .endSpec()
        .build()
    }
  }

  // Add a OwnerReference to the given resources making the pod an owner of them so when
  // the pod is deleted, the resources are garbage collected.
  @Since("3.1.1")
  def addOwnerReference(pod: Pod, resources: Seq[HasMetadata]): Unit = {
    if (pod != null) {
      val reference = new OwnerReferenceBuilder()
        .withName(pod.getMetadata.getName)
        .withApiVersion(pod.getApiVersion)
        .withUid(pod.getMetadata.getUid)
        .withKind(pod.getKind)
        .withController(true)
        .build()
      resources.foreach { resource =>
        val originalMetadata = resource.getMetadata
        originalMetadata.setOwnerReferences(Collections.singletonList(reference))
      }
    }
  }
}
