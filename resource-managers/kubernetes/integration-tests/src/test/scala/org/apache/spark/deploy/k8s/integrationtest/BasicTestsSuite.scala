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

import io.fabric8.kubernetes.api.model.Pod
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.{SparkFunSuite, TestUtils}
import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.SPARK_PI_MAIN_CLASS
import org.apache.spark.io.CompressionCodec
import org.apache.spark.launcher.SparkLauncher

private[spark] trait BasicTestsSuite { k8sSuite: KubernetesSuite =>

  import BasicTestsSuite._
  import KubernetesSuite.{k8sTestTag, localTestTag}
  import KubernetesSuite.{TIMEOUT, INTERVAL}

  test("SPARK-42190: Run SparkPi with local[*]", k8sTestTag) {
    sparkAppConf.set("spark.kubernetes.driver.master", "local[10]")
    runSparkApplicationAndVerifyCompletion(
      containerLocalSparkDistroExamplesJar,
      SPARK_PI_MAIN_CLASS,
      Seq("local[10]", "Pi is roughly 3"),
      Seq(),
      Array.empty[String],
      doBasicDriverPodCheck,
      _ => (),
      isJVM = true,
      executorPatience =
        Some((Some(PatienceConfiguration.Interval(Span(0, Seconds))), None)))
  }

  test("Run SparkPi with no resources", k8sTestTag) {
    runSparkPiAndVerifyCompletion()
  }

  test("Run SparkPi with no resources & statefulset allocation", k8sTestTag) {
    sparkAppConf.set("spark.kubernetes.allocation.pods.allocator", "statefulset")
    runSparkPiAndVerifyCompletion()
    // Verify there is no dangling statefulset
    // This depends on the garbage collection happening inside of K8s so give it some time.
    Eventually.eventually(TIMEOUT, INTERVAL) {
      val sets = kubernetesTestComponents.kubernetesClient
        .apps()
        .statefulSets()
        .inNamespace(kubernetesTestComponents.namespace)
        .list()
        .getItems
      sets.asScala.size shouldBe 0
    }
  }

  test("Run SparkPi with a very long application name.", k8sTestTag) {
    sparkAppConf.set("spark.app.name", "long" * 40)
    runSparkPiAndVerifyCompletion()
  }

  test("Use SparkLauncher.NO_RESOURCE", k8sTestTag) {
    sparkAppConf.setJars(Seq(containerLocalSparkDistroExamplesJar))
    runSparkPiAndVerifyCompletion(
      appResource = SparkLauncher.NO_RESOURCE)
  }

  test("Run SparkPi with a master URL without a scheme.", k8sTestTag) {
    val url = kubernetesTestComponents.kubernetesClient.getMasterUrl
    val k8sMasterUrl = if (url.getPort < 0) {
      s"k8s://${url.getHost}"
    } else {
      s"k8s://${url.getHost}:${url.getPort}"
    }
    sparkAppConf.set("spark.master", k8sMasterUrl)
    runSparkPiAndVerifyCompletion()
  }

  test("Run SparkPi with an argument.", k8sTestTag) {
    // This additional configuration with snappy is for SPARK-26995
    sparkAppConf
      .set("spark.io.compression.codec", CompressionCodec.SNAPPY)
    runSparkPiAndVerifyCompletion(appArgs = Array("5"))
  }

  test("Run SparkPi with custom labels, annotations, and environment variables.", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.driver.label.label1", "label1-value")
      .set("spark.kubernetes.driver.label.label2", "label2-value")
      .set("spark.kubernetes.driver.label.customAppIdLabelKey", "{{APP_ID}}")
      .set("spark.kubernetes.driver.annotation.annotation1", "annotation1-value")
      .set("spark.kubernetes.driver.annotation.annotation2", "annotation2-value")
      .set("spark.kubernetes.driver.annotation.customAppIdAnnotation", "{{APP_ID}}")
      .set("spark.kubernetes.driverEnv.ENV1", "VALUE1")
      .set("spark.kubernetes.driverEnv.ENV2", "VALUE2")
      .set("spark.kubernetes.executor.label.label1", "label1-value")
      .set("spark.kubernetes.executor.label.label2", "label2-value")
      .set("spark.kubernetes.executor.label.customAppIdLabelKey", "{{APP_ID}}")
      .set("spark.kubernetes.executor.annotation.annotation1", "annotation1-value")
      .set("spark.kubernetes.executor.annotation.annotation2", "annotation2-value")
      .set("spark.kubernetes.executor.annotation.customAppIdAnnotation", "{{APP_ID}}")
      .set("spark.executorEnv.ENV1", "VALUE1")
      .set("spark.executorEnv.ENV2", "VALUE2")

    runSparkPiAndVerifyCompletion(
      driverPodChecker = (driverPod: Pod) => {
        doBasicDriverPodCheck(driverPod)
        checkCustomSettings(driverPod)
      },
      executorPodChecker = (executorPod: Pod) => {
        doBasicExecutorPodCheck(executorPod)
        checkCustomSettings(executorPod)
      })
  }

  test("All pods have the same service account by default", k8sTestTag) {
    runSparkPiAndVerifyCompletion(
      executorPodChecker = (executorPod: Pod) => {
        doExecutorServiceAccountCheck(executorPod, kubernetesTestComponents.serviceAccountName)
      })
  }

  test("Run extraJVMOptions check on driver", k8sTestTag) {
    sparkAppConf
      .set("spark.driver.extraJavaOptions", "-Dspark.test.foo=spark.test.bar")
    runSparkJVMCheckAndVerifyCompletion(
      expectedJVMValue = Seq("(spark.test.foo,spark.test.bar)"))
  }

  test("SPARK-42474: Run extraJVMOptions JVM GC option check - G1GC", k8sTestTag) {
    sparkAppConf
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    runSparkJVMCheckAndVerifyCompletion(
      expectedJVMValue = Seq("JVM G1GC Flag: true"))
  }

  test("SPARK-42474: Run extraJVMOptions JVM GC option check - Other GC", k8sTestTag) {
    sparkAppConf
      .set("spark.driver.extraJavaOptions", "-XX:+UseParallelGC")
      .set("spark.executor.extraJavaOptions", "-XX:+UseParallelGC")
    runSparkJVMCheckAndVerifyCompletion(
      expectedJVMValue = Seq("JVM G1GC Flag: false"))
  }

  test("Run SparkRemoteFileTest using a remote data file", k8sTestTag, localTestTag) {
    assert(sys.props.contains("spark.test.home"), "spark.test.home is not set!")
    TestUtils.withHttpServer(sys.props("spark.test.home")) { baseURL =>
      sparkAppConf.set("spark.files", baseURL.toString +
          REMOTE_PAGE_RANK_DATA_FILE.replace(sys.props("spark.test.home"), "").substring(1))
      runSparkRemoteCheckAndVerifyCompletion(appArgs = Array(REMOTE_PAGE_RANK_FILE_NAME))
    }
  }

  test("SPARK-42769: All executor pods have SPARK_DRIVER_POD_IP env variable", k8sTestTag) {
    runSparkPiAndVerifyCompletion(
      executorPodChecker = (executorPod: Pod) => {
        doBasicExecutorPodCheck(executorPod)
        assert {
          executorPod.getSpec.getContainers.get(0).getEnv.asScala
            .exists(envVar => envVar.getName == "SPARK_DRIVER_POD_IP")
        }
      })
  }
}

private[spark] object BasicTestsSuite extends SparkFunSuite {
  val SPARK_PAGE_RANK_MAIN_CLASS: String = "org.apache.spark.examples.SparkPageRank"
  val CONTAINER_LOCAL_FILE_DOWNLOAD_PATH = "/var/spark-data/spark-files"
  val CONTAINER_LOCAL_DOWNLOADED_PAGE_RANK_DATA_FILE =
     s"$CONTAINER_LOCAL_FILE_DOWNLOAD_PATH/pagerank_data.txt"
  val REMOTE_PAGE_RANK_DATA_FILE = getTestResourcePath("pagerank_data.txt")
  val REMOTE_PAGE_RANK_FILE_NAME = "pagerank_data.txt"
}
