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
package org.apache.spark.deploy.k8s.integrationtest

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model._
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite._
import org.apache.spark.deploy.k8s.integrationtest.backend.minikube.MinikubeTestBackend

private[spark] trait VolumeSuite { k8sSuite: KubernetesSuite =>
  val IGNORE = Some((Some(PatienceConfiguration.Interval(Span(0, Seconds))), None))

  private def checkDisk(pod: Pod, path: String, expected: String) = {
    eventually(PatienceConfiguration.Timeout(Span(10, Seconds)), INTERVAL) {
      implicit val podName: String = pod.getMetadata.getName
      implicit val components: KubernetesTestComponents = kubernetesTestComponents
      assert(Utils.executeCommand("df", path).contains(expected))
    }
  }

  test("A driver-only Spark job with a tmpfs-backed localDir volume", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.driver.master", "local[10]")
      .set("spark.kubernetes.local.dirs.tmpfs", "true")
    runSparkApplicationAndVerifyCompletion(
      containerLocalSparkDistroExamplesJar,
      SPARK_PI_MAIN_CLASS,
      Seq("local[10]", "Pi is roughly 3"),
      Seq(),
      Array.empty[String],
      driverPodChecker = (driverPod: Pod) => {
        doBasicDriverPodCheck(driverPod)
        val path = driverPod.getSpec.getContainers.get(0).getEnv.asScala
          .filter(_.getName == "SPARK_LOCAL_DIRS").map(_.getValue).head
        checkDisk(driverPod, path, "tmpfs")
      },
      _ => (),
      isJVM = true,
      executorPatience = IGNORE)
  }

  test("A driver-only Spark job with a tmpfs-backed emptyDir data volume", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.driver.master", "local[10]")
      .set("spark.kubernetes.driver.volumes.emptyDir.data.mount.path", "/data")
      .set("spark.kubernetes.driver.volumes.emptyDir.data.options.medium", "Memory")
      .set("spark.kubernetes.driver.volumes.emptyDir.data.options.sizeLimit", "1G")
    runSparkApplicationAndVerifyCompletion(
      containerLocalSparkDistroExamplesJar,
      SPARK_PI_MAIN_CLASS,
      Seq("local[10]", "Pi is roughly 3"),
      Seq(),
      Array.empty[String],
      driverPodChecker = (driverPod: Pod) => {
        doBasicDriverPodCheck(driverPod)
        checkDisk(driverPod, "/data", "tmpfs")
      },
      _ => (),
      isJVM = true,
      executorPatience = IGNORE)
  }

  test("A driver-only Spark job with a disk-backed emptyDir volume", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.driver.master", "local[10]")
      .set("spark.kubernetes.driver.volumes.emptyDir.data.mount.path", "/data")
      .set("spark.kubernetes.driver.volumes.emptyDir.data.mount.sizeLimit", "1G")
    runSparkApplicationAndVerifyCompletion(
      containerLocalSparkDistroExamplesJar,
      SPARK_PI_MAIN_CLASS,
      Seq("local[10]", "Pi is roughly 3"),
      Seq(),
      Array.empty[String],
      driverPodChecker = (driverPod: Pod) => {
        doBasicDriverPodCheck(driverPod)
        checkDisk(driverPod, "/data", "/dev/")
      },
      _ => (),
      isJVM = true,
      executorPatience = IGNORE)
  }

  test("A driver-only Spark job with an OnDemand PVC volume", k8sTestTag) {
    val storageClassName = if (testBackend == MinikubeTestBackend) "standard" else "hostpath"
    val DRIVER_PREFIX = "spark.kubernetes.driver.volumes.persistentVolumeClaim"
    sparkAppConf
      .set("spark.kubernetes.driver.master", "local[10]")
      .set(s"$DRIVER_PREFIX.data.options.claimName", "OnDemand")
      .set(s"$DRIVER_PREFIX.data.options.storageClass", storageClassName)
      .set(s"$DRIVER_PREFIX.data.options.sizeLimit", "1Gi")
      .set(s"$DRIVER_PREFIX.data.mount.path", "/data")
      .set(s"$DRIVER_PREFIX.data.mount.readOnly", "false")
    runSparkApplicationAndVerifyCompletion(
      containerLocalSparkDistroExamplesJar,
      SPARK_PI_MAIN_CLASS,
      Seq("local[10]", "Pi is roughly 3"),
      Seq(),
      Array.empty[String],
      driverPodChecker = (driverPod: Pod) => {
        doBasicDriverPodCheck(driverPod)
        checkDisk(driverPod, "/data", "/dev/")
      },
      _ => (),
      isJVM = true,
      executorPatience = IGNORE)
  }

  test("A Spark job with tmpfs-backed localDir volumes", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.local.dirs.tmpfs", "true")
    runSparkApplicationAndVerifyCompletion(
      containerLocalSparkDistroExamplesJar,
      SPARK_PI_MAIN_CLASS,
      Seq("Pi is roughly 3"),
      Seq(),
      Array.empty[String],
      driverPodChecker = (driverPod: Pod) => {
        doBasicDriverPodCheck(driverPod)
        val path = driverPod.getSpec.getContainers.get(0).getEnv.asScala
          .filter(_.getName == "SPARK_LOCAL_DIRS").map(_.getValue).head
        checkDisk(driverPod, path, "tmpfs")
      },
      executorPodChecker = (executorPod: Pod) => {
        doBasicExecutorPodCheck(executorPod)
        val path = executorPod.getSpec.getContainers.get(0).getEnv.asScala
          .filter(_.getName == "SPARK_LOCAL_DIRS").map(_.getValue).head
        checkDisk(executorPod, path, "tmpfs")
      },
      isJVM = true)
  }

  test("A Spark job with two executors with OnDemand PVC volumes", k8sTestTag) {
    val storageClassName = if (testBackend == MinikubeTestBackend) "standard" else "hostpath"
    val EXECUTOR_PREFIX = "spark.kubernetes.executor.volumes.persistentVolumeClaim"
    sparkAppConf
      .set("spark.executor.instances", "2")
      .set(s"$EXECUTOR_PREFIX.data.options.claimName", "OnDemand")
      .set(s"$EXECUTOR_PREFIX.data.options.storageClass", storageClassName)
      .set(s"$EXECUTOR_PREFIX.data.options.sizeLimit", "1Gi")
      .set(s"$EXECUTOR_PREFIX.data.mount.path", "/data")
      .set(s"$EXECUTOR_PREFIX.data.mount.readOnly", "false")
    runSparkApplicationAndVerifyCompletion(
      containerLocalSparkDistroExamplesJar,
      SPARK_PI_MAIN_CLASS,
      Seq("Pi is roughly 3"),
      Seq(),
      Array.empty[String],
      _ => (),
      executorPodChecker = (executorPod: Pod) => {
        doBasicExecutorPodCheck(executorPod)
        checkDisk(executorPod, "/data", "/dev/")
      },
      isJVM = true)
  }
}
