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

import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient}
import io.fabric8.kubernetes.api.model.{PodBuilder, ServiceBuilder}
import io.fabric8.kubernetes.client.dsl.LogWatch
import org.apache.spark.deploy.kubernetes.ClientArguments
import org.apache.spark.{io, _}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

import collection.JavaConverters._
import org.apache.spark.util.Utils

/**
  * This is a simple extension to ClusterScheduler
  * */
private[spark] class KubernetesClusterScheduler(conf: SparkConf)
    extends Logging {
  logWarning("Created KubernetesClusterScheduler")

  val kubernetesHost = new java.net.URI(conf.get("spark.master"))
  var config =
    new ConfigBuilder().withMasterUrl(kubernetesHost.getHost()).build
  var client = new DefaultKubernetesClient(config)
  var myNs = client.namespaces().list()

  def start(args: ClientArguments): Unit = {
    startDriver(client, args)
    logWarning(myNs.toString())
  }

  def stop(): Unit = {}

  def startDriver(client: DefaultKubernetesClient,
                  args: ClientArguments): Unit = {
    println("###DRIVERSTART->" + args.userJar)
    var annotationMap = Map("pod.beta.kubernetes.io/init-containers" -> raw"""[
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

    val labelMap = Map("name" -> "spark-driver")
    val pod = new PodBuilder()
      .withNewMetadata()
      .withLabels(labelMap.asJava)
      .withName("spark-driver")
      .withAnnotations(annotationMap.asJava)
      .endMetadata()
      .withNewSpec()
      .withRestartPolicy("OnFailure")
      .addNewContainer()
      .withName("spark-driver")
      .withImage("foxish/k8s-spark-driver:latest")
      .withImagePullPolicy("Always")
      .withCommand("/opt/spark/bin/spark-submit")
      .withArgs("--class=org.apache.spark.examples.SparkPi",
                s"--master=$kubernetesHost",
                "--executor-memory=2G",
                "--num-executors=8",
                "/work-dir/client.jar",
                "10000")
      .addNewPort()
      .withContainerPort(80)
      .endPort()
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
      .endSpec()
      .build()

    client.pods().inNamespace("default").withName("spark-driver").create(pod)

    var svc = new ServiceBuilder()
      .withNewMetadata()
      .withLabels(labelMap.asJava)
      .withName("spark-driver-svc")
      .endMetadata()
      .withNewSpec()
      .addNewPort()
      .withPort(4040)
      .withNewTargetPort()
      .withIntVal(4040)
      .endTargetPort()
      .endPort()
      .withSelector(labelMap.asJava)
      .withType("LoadBalancer")
      .endSpec()
      .build()

    client
      .services()
      .inNamespace("default")
      .withName("spark-driver-svc")
      .create(svc)

//    try {
//      while (true) {
//        client
//          .pods()
//          .inNamespace("default")
//          .withName("spark-driver")
//          .tailingLines(10)
//          .watchLog(System.out)
//        Thread.sleep(5 * 1000)
//      }
//    } catch {
//      case e: Exception => logError(e.getMessage)
//    }
  }

  def getNamespace(): String = {
    return "default"
  }

  def generateJobName(): String = {
    // let the job name be a sha random alphanumeric string
    // + the timestamp.
    return ""
  }

  def generateDriverName(): String = {
    // TODO: Fill
    return ""
  }

  def generateSvcName(): String = {
    // TODO: Fill
    return ""
  }
}
