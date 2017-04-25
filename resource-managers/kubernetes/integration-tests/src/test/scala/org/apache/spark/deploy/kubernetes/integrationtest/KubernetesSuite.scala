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
package org.apache.spark.deploy.kubernetes.integrationtest

import java.nio.file.Paths

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.scalatest.Suite
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.{Minutes, Seconds, Span}

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.kubernetes.integrationtest.docker.SparkDockerImageBuilder
import org.apache.spark.deploy.kubernetes.integrationtest.minikube.Minikube

private[spark] class KubernetesSuite extends SparkFunSuite {

  override def beforeAll(): Unit = {
    Minikube.startMinikube()
    new SparkDockerImageBuilder(Minikube.getDockerEnv).buildSparkDockerImages()
  }

  override def afterAll(): Unit = {
    if (!System.getProperty("spark.docker.test.persistMinikube", "false").toBoolean) {
      Minikube.deleteMinikube()
    }
  }

  override def nestedSuites: scala.collection.immutable.IndexedSeq[Suite] = {
      Vector(
        new KubernetesV1Suite,
        new KubernetesV2Suite)
  }
}

private[spark] object KubernetesSuite {
  val EXAMPLES_JAR_FILE = Paths.get("target", "integration-tests-spark-jobs")
    .toFile
    .listFiles()(0)

  val HELPER_JAR_FILE = Paths.get("target", "integration-tests-spark-jobs-helpers")
    .toFile
    .listFiles()(0)
  val SUBMITTER_LOCAL_MAIN_APP_RESOURCE = s"file://${EXAMPLES_JAR_FILE.getAbsolutePath}"
  val CONTAINER_LOCAL_MAIN_APP_RESOURCE = s"local:///opt/spark/examples/" +
    s"integration-tests-jars/${EXAMPLES_JAR_FILE.getName}"
  val CONTAINER_LOCAL_HELPER_JAR_PATH = s"local:///opt/spark/examples/" +
    s"integration-tests-jars/${HELPER_JAR_FILE.getName}"

  val TEST_EXISTENCE_FILE = Paths.get("test-data", "input.txt").toFile
  val TEST_EXISTENCE_FILE_CONTENTS = Files.toString(TEST_EXISTENCE_FILE, Charsets.UTF_8)
  val TIMEOUT = PatienceConfiguration.Timeout(Span(2, Minutes))
  val INTERVAL = PatienceConfiguration.Interval(Span(2, Seconds))
  val SPARK_PI_MAIN_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.SparkPiWithInfiniteWait"
  val FILE_EXISTENCE_MAIN_CLASS = "org.apache.spark.deploy.kubernetes" +
    ".integrationtest.jobs.FileExistenceTest"
}
