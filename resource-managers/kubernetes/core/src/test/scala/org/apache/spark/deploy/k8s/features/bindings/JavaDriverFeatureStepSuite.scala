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
package org.apache.spark.deploy.k8s.features.bindings

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpecificConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.PythonMainAppResource

class JavaDriverFeatureStepSuite extends SparkFunSuite {

  test("Java Step modifies container correctly") {
    val baseDriverPod = SparkPod.initialPod()
    val sparkConf = new SparkConf(false)
    val kubernetesConf = KubernetesConf(
      sparkConf,
      KubernetesDriverSpecificConf(
        Some(PythonMainAppResource("local:///main.jar")),
        "test-class",
        "java-runner",
        Seq("5 7")),
      appResourceNamePrefix = "",
      appId = "",
      roleLabels = Map.empty,
      roleAnnotations = Map.empty,
      roleSecretNamesToMountPaths = Map.empty,
      roleSecretEnvNamesToKeyRefs = Map.empty,
      roleEnvs = Map.empty,
      roleVolumes = Nil,
      sparkFiles = Seq.empty[String])

    val step = new JavaDriverFeatureStep(kubernetesConf)
    val driverPod = step.configurePod(baseDriverPod).pod
    val driverContainerwithJavaStep = step.configurePod(baseDriverPod).container
    assert(driverContainerwithJavaStep.getArgs.size === 7)
    val args = driverContainerwithJavaStep
      .getArgs.asScala
    assert(args === List(
      "driver",
      "--properties-file", SPARK_CONF_PATH,
      "--class", "test-class",
      "spark-internal", "5 7"))
  }
}
