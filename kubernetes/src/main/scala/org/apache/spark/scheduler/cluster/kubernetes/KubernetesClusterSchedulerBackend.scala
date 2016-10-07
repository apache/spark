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

package org.apache.spark.scheduler.cluster.kubernetes

import collection.JavaConverters._
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.extensions.JobBuilder
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient}
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.cluster._
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.Utils

import scala.collection.mutable
import scala.util.Random

private[spark] class KubernetesClusterSchedulerBackend(
                                                  scheduler: TaskSchedulerImpl,
                                                  sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {
  val config = new ConfigBuilder().withMasterUrl("https://kubernetes").build
  val client = new DefaultKubernetesClient(config)
  val DEFAULT_NUMBER_EXECUTORS = 2
  val sparkExecutorName = s"spark-executor-${Random.alphanumeric take 5 mkString("")}".toLowerCase()
  var executorPods = mutable.ArrayBuffer[String]()

  val sparkDistUri = sc.getConf.get("spark.kubernetes.distribution.uri")
  val sparkDriverImage = sc.getConf.get("spark.kubernetes.driver.image")
  val clientJarUri = sc.getConf.get("spark.executor.jar")
  val ns = sc.getConf.get("spark.kubernetes.namespace")

  override def start() {
    super.start()
    var i = 0
    for(i <- 1 to getInitialTargetExecutorNumber(sc.conf)){
      executorPods += createExecutorPod(i)
    }
    None
  }

  override def stop(): Unit = {
    for (i <- 0 to executorPods.length) {
      client.pods().inNamespace(ns).withName(executorPods(i)).delete()
    }
    super.stop()
  }

  // Dynamic allocation interfaces
  override def doRequestTotalExecutors(requestedTotal: Int): scala.concurrent.Future[Boolean] = {
    return super.doRequestTotalExecutors(requestedTotal)
  }

  override def doKillExecutors(executorIds: Seq[String]): scala.concurrent.Future[Boolean] = {
    return super.doKillExecutors(executorIds)
  }

  def getInitialTargetExecutorNumber(conf: SparkConf,
                                     numExecutors: Int =
                                     DEFAULT_NUMBER_EXECUTORS): Int = {
    if (Utils.isDynamicAllocationEnabled(conf)) {
      val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)
      val initialNumExecutors =
        Utils.getDynamicAllocationInitialExecutors(conf)
      val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
      require(
        initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number " +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    } else {
      val targetNumExecutors =
        sys.env
          .get("SPARK_EXECUTOR_INSTANCES")
          .map(_.toInt)
          .getOrElse(numExecutors)
      conf.get(EXECUTOR_INSTANCES).getOrElse(targetNumExecutors)
    }
  }

  def createExecutorPod(executorNum: Int): String = {
    // create a single k8s executor pod.
    var annotationMap = Map(
      "pod.beta.kubernetes.io/init-containers" -> raw"""[
                {
                    "name": "client-fetch",
                    "image": "busybox",
                    "command": ["wget", "-O", "/work-dir/client.jar", "$clientJarUri"],
                    "volumeMounts": [
                        {
                            "name": "workdir",
                            "mountPath": "/work-dir"
                        }
                    ]
                },
                {
                    "name": "distro-fetch",
                    "image": "busybox",
                    "command": ["wget", "-O", "/work-dir/spark.tgz", "$sparkDistUri"],
                    "volumeMounts": [
                        {
                            "name": "workdir",
                            "mountPath": "/work-dir"
                        }
                    ]
                },
                {
                    "name": "setup",
                    "image": "$sparkDriverImage",
                    "command": ["./install.sh"],
                    "volumeMounts": [
                        {
                            "name": "workdir",
                            "mountPath": "/work-dir"
                        },
                        {
                            "name": "opt",
                            "mountPath": "/opt"
                        }
                    ]
                }
            ]""")


    val labelMap = Map("type" -> "spark-executor")
    val podName = s"$sparkExecutorName-$executorNum"
    var pod = new PodBuilder()
      .withNewMetadata()
      .withLabels(labelMap.asJava)
      .withName(podName)
      .withAnnotations(annotationMap.asJava)
      .endMetadata()
      .withNewSpec()
      .withRestartPolicy("OnFailure")
      .addNewContainer().withName("spark-executor").withImage(sparkDriverImage)
      .withImagePullPolicy("Always")
      .withCommand("/opt/spark/bin/spark-class")
      .withArgs("org.apache.spark.executor.CoarseGrainedExecutorBackend",
        "--driver-url", s"$driverURL",
        "--executor-id", s"$executorNum",
        "--hostname", "localhost",
        "--cores", "1",
        "--app-id", "1") //TODO: change app-id per application and pass from driver.
      .withVolumeMounts()
      .addNewVolumeMount()
      .withName("workdir")
      .withMountPath("/work-dir")
      .endVolumeMount()
      .addNewVolumeMount()
      .withName("opt")
      .withMountPath("/opt")
      .endVolumeMount()
      .endContainer()
      .withVolumes()
      .addNewVolume()
      .withName("workdir")
      .withNewEmptyDir()
      .endEmptyDir()
      .endVolume()
      .addNewVolume()
      .withName("opt")
      .withNewEmptyDir()
      .endEmptyDir()
      .endVolume()
      .endSpec().build()
    client.pods().inNamespace(ns).withName(podName).create(pod)
    return podName
  }


  protected def driverURL: String = {
    if (conf.contains("spark.testing")) {
      "driverURL"
    } else {
      RpcEndpointAddress(
        conf.get("spark.driver.host"),
        conf.get("spark.driver.port").toInt,
        CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
    }
  }

  override def getDriverLogUrls: Option[Map[String, String]] = {
    var driverLogs: Option[Map[String, String]] = None
    driverLogs
  }
}
