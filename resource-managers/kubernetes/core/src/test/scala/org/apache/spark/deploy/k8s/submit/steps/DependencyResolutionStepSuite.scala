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
package org.apache.spark.deploy.k8s.submit.steps

import java.io.File

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, PodBuilder}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec

class DependencyResolutionStepSuite extends SparkFunSuite {

  private val SPARK_JARS = Seq(
    "hdfs://localhost:9000/apps/jars/jar1.jar",
    "file:///home/user/apps/jars/jar2.jar",
    "local:///var/apps/jars/jar3.jar")

  private val SPARK_FILES = Seq(
    "file:///home/user/apps/files/file1.txt",
    "hdfs://localhost:9000/apps/files/file2.txt",
    "local:///var/apps/files/file3.txt")

  private val JARS_DOWNLOAD_PATH = "/mnt/spark-data/jars"
  private val FILES_DOWNLOAD_PATH = "/mnt/spark-data/files"

  test("Added dependencies should be resolved in Spark configuration and environment") {
    val dependencyResolutionStep = new DependencyResolutionStep(
      SPARK_JARS,
      SPARK_FILES,
      JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_PATH)
    val driverPod = new PodBuilder().build()
    val baseDriverSpec = KubernetesDriverSpec(
      driverPod = driverPod,
      driverContainer = new ContainerBuilder().build(),
      driverSparkConf = new SparkConf(false),
      otherKubernetesResources = Seq.empty[HasMetadata])
    val preparedDriverSpec = dependencyResolutionStep.configureDriver(baseDriverSpec)
    assert(preparedDriverSpec.driverPod === driverPod)
    assert(preparedDriverSpec.otherKubernetesResources.isEmpty)
    val resolvedSparkJars = preparedDriverSpec.driverSparkConf.get("spark.jars").split(",").toSet
    val expectedResolvedSparkJars = Set(
      "hdfs://localhost:9000/apps/jars/jar1.jar",
      s"$JARS_DOWNLOAD_PATH/jar2.jar",
      "/var/apps/jars/jar3.jar")
    assert(resolvedSparkJars === expectedResolvedSparkJars)
    val resolvedSparkFiles = preparedDriverSpec.driverSparkConf.get("spark.files").split(",").toSet
    val expectedResolvedSparkFiles = Set(
      s"$FILES_DOWNLOAD_PATH/file1.txt",
      s"hdfs://localhost:9000/apps/files/file2.txt",
      s"/var/apps/files/file3.txt")
    assert(resolvedSparkFiles === expectedResolvedSparkFiles)
    val driverEnv = preparedDriverSpec.driverContainer.getEnv.asScala
    assert(driverEnv.size === 1)
    assert(driverEnv.head.getName === ENV_MOUNTED_CLASSPATH)
    val resolvedDriverClasspath = driverEnv.head.getValue.split(File.pathSeparator).toSet
    val expectedResolvedDriverClasspath = Set(
      s"$JARS_DOWNLOAD_PATH/jar1.jar",
      s"$JARS_DOWNLOAD_PATH/jar2.jar",
      "/var/apps/jars/jar3.jar")
    assert(resolvedDriverClasspath === expectedResolvedDriverClasspath)
  }
}
