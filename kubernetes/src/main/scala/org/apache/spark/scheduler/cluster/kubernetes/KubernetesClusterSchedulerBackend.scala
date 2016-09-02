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
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.Utils

private[spark] class KubernetesClusterSchedulerBackend(
                                                  scheduler: TaskSchedulerImpl,
                                                  sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {
  val config = new ConfigBuilder().withMasterUrl("https://kubernetes").build
  val client = new DefaultKubernetesClient(config)
  val DEFAULT_NUMBER_EXECUTORS = 2
  var NO_EXECUTORS = false

  override def start() {
    logWarning("Starting scheduler backendaa2")
    super.start()
    var i = 0


    logWarning("###->initialexecutors=" + getInitialTargetExecutorNumber(sc.conf).toString())
    for(i <- 1 to getInitialTargetExecutorNumber(sc.conf)){
      createExecutorPod(i)
    }
    None
  }

  override def stop(): Unit = {
    super.stop()
    client.pods().inNamespace("default").withName("spark-driver").delete()
    client
      .services()
      .inNamespace("default")
      .withName("spark-driver-svc")
      .delete()
    scheduler.stop()
  }

  // Dynamic allocation interfaces
  override def doRequestTotalExecutors(requestedTotal: Int): scala.concurrent.Future[Boolean] = {
    logWarning("###->doRequestTotalExecutors" + requestedTotal.toString())

    // time to create it.
    if (!NO_EXECUTORS) {
      NO_EXECUTORS = true
      createExecutorPod(requestedTotal)
    }
    return super.doRequestTotalExecutors(requestedTotal)
  }

  override def doKillExecutors(executorIds: Seq[String]): scala.concurrent.Future[Boolean] = {
    logWarning("###->doKillExecutors" + executorIds.toString())
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
      // System property can override environment variable.
      conf.get(EXECUTOR_INSTANCES).getOrElse(targetNumExecutors)
    }
  }

  def createExecutorPod(executorNum: Int): Unit = {
    // create a single k8s executor pod.
    var annotationMap = Map(
      "pod.beta.kubernetes.io/init-containers" -> raw"""[
                {
                    "name": "client-fetch",
                    "image": "busybox",
                    "command": ["wget", "-O", "/work-dir/client.jar", "http://storage.googleapis.com/foxish-spark-distro/original-spark-examples_2.11-2.1.0-SNAPSHOT.jar"],
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
                    "command": ["wget", "-O", "/work-dir/spark.tgz", "http://storage.googleapis.com/foxish-spark-distro/spark.tgz"],
                    "volumeMounts": [
                        {
                            "name": "workdir",
                            "mountPath": "/work-dir"
                        }
                    ]
                },
                {
                    "name": "setup",
                    "image": "foxish/k8s-spark-driver:latest",
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


    val labelMap = Map("name" -> "spark-executor")
    var pod = new PodBuilder()
      .withNewMetadata()
      .withLabels(labelMap.asJava)
      .withName(s"spark-executor-$executorNum")
      .withAnnotations(annotationMap.asJava)
      .endMetadata()
      .withNewSpec()
      .withRestartPolicy("OnFailure")
      .addNewContainer().withName("spark-executor").withImage("foxish/k8s-spark-executor:latest")
      .withImagePullPolicy("Always")
      .withCommand("/opt/spark/bin/spark-class")
      .withArgs("org.apache.spark.executor.CoarseGrainedExecutorBackend",
        "--driver-url", s"$driverURL",
        "--executor-id", s"$executorNum", "--hostname", "localhost", "--cores", "1",
        "--app-id", "1")
      .addNewPort().withContainerPort(80).endPort()
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
    client.pods().inNamespace("default").withName(s"spark-executor-$executorNum").create(pod)
    logWarning("## BUILDING POD FROM WITHIN ##")
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
