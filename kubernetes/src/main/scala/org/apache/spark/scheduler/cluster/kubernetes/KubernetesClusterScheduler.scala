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

import java.io.File
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient}
import io.fabric8.kubernetes.api.model.{PodBuilder, ServiceBuilder}
import io.fabric8.kubernetes.client.dsl.LogWatch
import org.apache.spark.deploy.Command
import org.apache.spark.deploy.kubernetes.ClientArguments
import org.apache.spark.{io, _}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

import collection.JavaConverters._
import org.apache.spark.util.Utils

import scala.util.Random

/**
  * This is a simple extension to ClusterScheduler
  * */
private[spark] class KubernetesClusterScheduler(conf: SparkConf)
    extends Logging {
  private val DEFAULT_SUPERVISE = false
  private val DEFAULT_MEMORY = Utils.DEFAULT_DRIVER_MEM_MB // mb
  private val DEFAULT_CORES = 1.0

  logInfo("Created KubernetesClusterScheduler instance")
  var client = setupKubernetesClient()
  val driverName = s"spark-driver-${Random.alphanumeric take 5 mkString("")}".toLowerCase()
  val svcName = s"spark-svc-${Random.alphanumeric take 5 mkString("")}".toLowerCase()
  val instances = conf.get(EXECUTOR_INSTANCES).getOrElse(2) //TODO: default 2???

  logWarning("instances: " +  instances)

  def start(args: ClientArguments): Unit = {
    startDriver(client, args)
  }

  def stop(): Unit = {
    client.pods().inNamespace(getNamespace()).withName(driverName).delete()
    client
      .services()
      .inNamespace(getNamespace())
      .withName(svcName)
      .delete()
  }

  def startDriver(client: KubernetesClient,
                  args: ClientArguments): Unit = {
    logInfo("Starting spark driver on kubernetes cluster")
    val driverDescription = buildDriverDescription(args)

    // This is the URL of the spark distro.
    val sparkDistUri = Option(System.getenv("SPARK_DISTRO_URI")).getOrElse {
      throw new SparkException("Spark distribution not set, please set the SPARK_DISTRO_URI environment variable to " +
        "a runnable spark archive.")
    }

    // This is the URL of the driver pod's image.
    // Any image may be supplied as long as it contains a
    // ./install.sh file which is executable and sets up the
    // spark environment in /opt/spark.
    val sparkDriverImage = Option(System.getenv("SPARK_DRIVER_IMG")).getOrElse {
      throw new SparkException("Spark driver image not set, please set the SPARK_DRIVER_IMG environment variable to " +
        "a spark driver image.")
    }

    // This is the URL of the client jar.
    val clientJarUri = args.userJar
    conf.setExecutorEnv("spark.executor.jar", clientJarUri)
    conf.setExecutorEnv("spark.kubernetes.namespace", getNamespace())
    conf.setExecutorEnv("spark.kubernetes.driver.image", sparkDriverImage)
    conf.setExecutorEnv("spark.kubernetes.distribution.uri", sparkDistUri)

    // This is the kubernetes master we're launching on.
    val kubernetesHost = "k8s://" + client.getMasterUrl().getHost()
    logInfo("Using as kubernetes-master: " + kubernetesHost.toString())

    var annotationMap = Map("pod.beta.kubernetes.io/init-containers" -> raw"""[
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


    val labelMap = Map("type" -> "spark-driver")
    val pod = new PodBuilder()
      .withNewMetadata()
      .withLabels(labelMap.asJava)
      .withName(driverName)
      .withAnnotations(annotationMap.asJava)
      .endMetadata()
      .withNewSpec()
      .withRestartPolicy("OnFailure")
      .addNewContainer()
      .withName("spark-driver")
      .withImage(sparkDriverImage)
      .withImagePullPolicy("Always")
      .withCommand("/opt/spark/bin/spark-submit")
      .withArgs(s"--class=${args.userClass}",
                s"--master=$kubernetesHost",
                s"--executor-memory=${driverDescription.mem}",
                s"--conf=spark.executor.jar=$clientJarUri",
                s"--conf=spark.executor.instances=$instances",
                s"--conf=spark.kubernetes.namespace=${getNamespace()}",
                s"--conf=spark.kubernetes.driver.image=$sparkDriverImage",
                s"--conf=spark.kubernetes.distribution.uri=$sparkDistUri",
                "/work-dir/client.jar",
                args.userArgs.mkString(" "))
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
    client.pods().inNamespace(getNamespace()).withName(driverName).create(pod)

    var svc = new ServiceBuilder()
      .withNewMetadata()
      .withLabels(labelMap.asJava)
      .withName(svcName)
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
      .inNamespace(getNamespace())
      .withName(svcName)
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

  def setupKubernetesClient(): KubernetesClient = {
      val sparkMaster = new java.net.URI(conf.get("spark.master"))
      if (sparkMaster.getHost() == "default") {
        return new DefaultKubernetesClient()
      } else {
        var config = new ConfigBuilder().withMasterUrl(sparkMaster.getHost()).build
        var client = new DefaultKubernetesClient(config)
        return client
      }
  }

  def getNamespace(): String = {
    var kubernetesNamespace = System.getenv("K8S_NAMESPACE")
    if (kubernetesNamespace == null) {
      kubernetesNamespace = "default"
    }
    return kubernetesNamespace
  }

  private def buildDriverDescription(args: ClientArguments): KubernetesDriverDescription = {
    // Required fields, including the main class because python is not yet supported
    val appResource = Option(args.userJar).getOrElse {
      throw new SparkException("Application jar is missing.")
    }
    val mainClass = Option(args.userClass).getOrElse {
      throw new SparkException("Main class is missing.")
    }

    // Optional fields
    val driverExtraJavaOptions = conf.getOption("spark.driver.extraJavaOptions")
    val driverExtraClassPath = conf.getOption("spark.driver.extraClassPath")
    val driverExtraLibraryPath = conf.getOption("spark.driver.extraLibraryPath")
    val superviseDriver = conf.getOption("spark.driver.supervise")
    val driverMemory = conf.getOption("spark.driver.memory")
    val driverCores = conf.getOption("spark.driver.cores")
    val name = conf.getOption("spark.app.name").getOrElse("default")
    val appArgs = args.userArgs

    // Construct driver description
    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(conf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = new Command(
      mainClass, appArgs, null, extraClassPath, extraLibraryPath, javaOpts)
    val actualSuperviseDriver = superviseDriver.map(_.toBoolean).getOrElse(DEFAULT_SUPERVISE)
    val actualDriverMemory = driverMemory.map(Utils.memoryStringToMb).getOrElse(DEFAULT_MEMORY)
    val actualDriverCores = driverCores.map(_.toDouble).getOrElse(DEFAULT_CORES)
    val submitDate = new Date()

    new KubernetesDriverDescription(
      name, appResource, actualDriverMemory, actualDriverCores, actualSuperviseDriver,
      command, submitDate)
  }
}
