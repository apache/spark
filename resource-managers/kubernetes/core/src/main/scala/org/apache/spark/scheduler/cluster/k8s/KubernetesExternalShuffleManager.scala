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

package org.apache.spark.scheduler.cluster.k8s

import io.fabric8.kubernetes.api.model.{Pod, Volume, VolumeBuilder, VolumeMount, VolumeMountBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.internal.readiness.Readiness
import org.apache.commons.io.FilenameUtils
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.ConfigurationUtils
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.internal.Logging
import org.apache.spark.network.shuffle.kubernetes.KubernetesExternalShuffleClient
import org.apache.spark.util.Utils

private[spark] trait KubernetesExternalShuffleManager {

  def start(appId: String): Unit

  def stop(): Unit

  /**
   * Returns the properties that should be applied for this executor pod, given that
   * this executor will need to communicate with an external shuffle service.
   *
   * In practice, this seq will always have a size of 1, but since this method's semantics are that
   * the returned values are key-value pairs to apply as properties, it is clearer to express
   * this as a collection.
   */
  def getShuffleServiceConfigurationForExecutor(executorPod: Pod): Seq[(String, String)]

  def getExecutorShuffleDirVolumesWithMounts: Seq[(Volume, VolumeMount)]

}

private[spark] class KubernetesExternalShuffleManagerImpl(
    sparkConf: SparkConf,
    client: KubernetesClient,
    shuffleClient: KubernetesExternalShuffleClient)
    extends KubernetesExternalShuffleManager with Logging {

  private val shuffleNamespace = sparkConf.get(KUBERNETES_SHUFFLE_NAMESPACE)
  private val shufflePodLabels = ConfigurationUtils.parseKeyValuePairs(
      sparkConf.get(KUBERNETES_SHUFFLE_LABELS),
      KUBERNETES_SHUFFLE_LABELS.key,
      "shuffle-labels")
  if (shufflePodLabels.isEmpty) {
    throw new SparkException(s"Dynamic allocation enabled " +
        s"but no ${KUBERNETES_SHUFFLE_LABELS.key} specified")
  }
  private val externalShufflePort = sparkConf.getInt("spark.shuffle.service.port", 7337)
  private val shuffleDirs = Utils.getConfiguredLocalDirs(sparkConf)
  private var shufflePodCache = scala.collection.mutable.Map[String, String]()
  private var watcher: Watch = _

  override def start(appId: String): Unit = {
    // seed the initial cache.
    val pods = client.pods()
      .inNamespace(shuffleNamespace)
      .withLabels(shufflePodLabels.asJava)
      .list()
    pods.getItems.asScala.foreach {
      pod =>
        if (Readiness.isReady(pod)) {
          addShufflePodToCache(pod)
        } else {
          logWarning(s"Found unready shuffle pod ${pod.getMetadata.getName} " +
            s"on node ${pod.getSpec.getNodeName}")
        }
    }

    watcher = client
      .pods()
      .inNamespace(shuffleNamespace)
      .withLabels(shufflePodLabels.asJava)
      .watch(new Watcher[Pod] {
        override def eventReceived(action: Watcher.Action, p: Pod): Unit = {
          action match {
            case Action.DELETED | Action.ERROR =>
              shufflePodCache.remove(p.getSpec.getNodeName)
            case Action.ADDED | Action.MODIFIED if Readiness.isReady(p) =>
              addShufflePodToCache(p)
          }
        }
        override def onClose(e: KubernetesClientException): Unit = {}
      })
    shuffleClient.init(appId)
  }

  private def addShufflePodToCache(pod: Pod): Unit = shufflePodCache.synchronized {
    if (shufflePodCache.contains(pod.getSpec.getNodeName)) {
      val registeredPodName = shufflePodCache.get(pod.getSpec.getNodeName).get
      logError(s"Ambiguous specification of shuffle service pod. " +
        s"Found multiple matching pods: ${pod.getMetadata.getName}, " +
        s"${registeredPodName} on ${pod.getSpec.getNodeName}")

      throw new SparkException(s"Ambiguous specification of shuffle service pod. " +
        s"Found multiple matching pods: ${pod.getMetadata.getName}, " +
        s"${registeredPodName} on ${pod.getSpec.getNodeName}")
    } else {
        shufflePodCache(pod.getSpec.getNodeName) = pod.getStatus.getPodIP
    }
  }

  override def stop(): Unit = {
    watcher.close()
    shuffleClient.close()
  }

  override def getShuffleServiceConfigurationForExecutor(executorPod: Pod)
      : Seq[(String, String)] = {
    val nodeName = executorPod.getSpec.getNodeName
    val shufflePodIp = shufflePodCache.synchronized {
      shufflePodCache.get(nodeName).getOrElse(
          throw new SparkException(s"Unable to find shuffle pod on node $nodeName"))
    }
    // Inform the shuffle pod about this application so it can watch.
    shuffleClient.registerDriverWithShuffleService(shufflePodIp, externalShufflePort)
    Seq((SPARK_SHUFFLE_SERVICE_HOST.key, shufflePodIp))
  }

  override def getExecutorShuffleDirVolumesWithMounts(): Seq[(Volume, VolumeMount)] = {
    // TODO: Using hostPath for the local directory will also make it such that the
    // other uses of the local directory - broadcasting and caching - will also write
    // to the directory that the shuffle service is aware of. It would be better for
    // these directories to be separate so that the lifetime of the non-shuffle scratch
    // space is tied to an emptyDir instead of the hostPath. This requires a change in
    // core Spark as well.
    shuffleDirs.zipWithIndex.map {
      case (shuffleDir, shuffleDirIndex) =>
        val volumeName = s"$shuffleDirIndex-${FilenameUtils.getBaseName(shuffleDir)}"
        val volume = new VolumeBuilder()
          .withName(volumeName)
          .withNewHostPath(shuffleDir)
          .build()
        val volumeMount = new VolumeMountBuilder()
          .withName(volumeName)
          .withMountPath(shuffleDir)
          .build()
        (volume, volumeMount)
    }
  }
}

