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
    "apps/jars/jar1.jar",
    "local:///var/apps/jars/jar2.jar")

  private val SPARK_FILES = Seq(
    "apps/files/file1.txt",
    "local:///var/apps/files/file2.txt")

  test("Added dependencies should be resolved in Spark configuration and environment") {
    val dependencyResolutionStep = new DependencyResolutionStep(
      SPARK_JARS,
      SPARK_FILES)
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
      "apps/jars/jar1.jar",
      "/var/apps/jars/jar2.jar")
    assert(resolvedSparkJars === expectedResolvedSparkJars)
    val resolvedSparkFiles = preparedDriverSpec.driverSparkConf.get("spark.files").split(",").toSet
    val expectedResolvedSparkFiles = Set(
      "apps/files/file1.txt",
      "/var/apps/files/file2.txt")
    assert(resolvedSparkFiles === expectedResolvedSparkFiles)
    val driverEnv = preparedDriverSpec.driverContainer.getEnv.asScala
    assert(driverEnv.size === 1)
    assert(driverEnv.head.getName === ENV_MOUNTED_CLASSPATH)
    val resolvedDriverClasspath = driverEnv.head.getValue.split(File.pathSeparator).toSet
    val expectedResolvedDriverClasspath = expectedResolvedSparkJars
    assert(resolvedDriverClasspath === expectedResolvedDriverClasspath)
  }
}
