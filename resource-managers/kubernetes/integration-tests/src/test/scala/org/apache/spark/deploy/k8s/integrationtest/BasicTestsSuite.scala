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

import io.fabric8.kubernetes.api.model.Pod

import org.apache.spark.launcher.SparkLauncher

private[spark] trait BasicTestsSuite { k8sSuite: KubernetesSuite =>

  import BasicTestsSuite._
  import KubernetesSuite.k8sTestTag

  test("Run SparkPi with no resources", k8sTestTag) {
    runSparkPiAndVerifyCompletion()
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
      .set("spark.io.compression.codec", "snappy")
    runSparkPiAndVerifyCompletion(appArgs = Array("5"))
  }

  test("Run SparkPi with custom labels, annotations, and environment variables.", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.driver.label.label1", "label1-value")
      .set("spark.kubernetes.driver.label.label2", "label2-value")
      .set("spark.kubernetes.driver.annotation.annotation1", "annotation1-value")
      .set("spark.kubernetes.driver.annotation.annotation2", "annotation2-value")
      .set("spark.kubernetes.driverEnv.ENV1", "VALUE1")
      .set("spark.kubernetes.driverEnv.ENV2", "VALUE2")
      .set("spark.kubernetes.executor.label.label1", "label1-value")
      .set("spark.kubernetes.executor.label.label2", "label2-value")
      .set("spark.kubernetes.executor.annotation.annotation1", "annotation1-value")
      .set("spark.kubernetes.executor.annotation.annotation2", "annotation2-value")
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

  test("Run extraJVMOptions check on driver", k8sTestTag) {
    sparkAppConf
      .set("spark.driver.extraJavaOptions", "-Dspark.test.foo=spark.test.bar")
    runSparkJVMCheckAndVerifyCompletion(
      expectedJVMValue = Seq("(spark.test.foo,spark.test.bar)"))
  }

  test("Run SparkRemoteFileTest using a remote data file", k8sTestTag) {
    sparkAppConf
      .set("spark.files", REMOTE_PAGE_RANK_DATA_FILE)
    runSparkRemoteCheckAndVerifyCompletion(appArgs = Array(REMOTE_PAGE_RANK_FILE_NAME))
  }
}

private[spark] object BasicTestsSuite {
  val SPARK_PAGE_RANK_MAIN_CLASS: String = "org.apache.spark.examples.SparkPageRank"
  val CONTAINER_LOCAL_FILE_DOWNLOAD_PATH = "/var/spark-data/spark-files"
  val CONTAINER_LOCAL_DOWNLOADED_PAGE_RANK_DATA_FILE =
     s"$CONTAINER_LOCAL_FILE_DOWNLOAD_PATH/pagerank_data.txt"
  val REMOTE_PAGE_RANK_DATA_FILE =
    "https://storage.googleapis.com/spark-k8s-integration-tests/files/pagerank_data.txt"
  val REMOTE_PAGE_RANK_FILE_NAME = "pagerank_data.txt"
}
