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
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.RMainAppResource

class RDriverFeatureStepSuite extends SparkFunSuite {

  test("R Step modifies container correctly") {
    val expectedMainResource = "/main.R"
    val mainResource = "local:///main.R"
    val baseDriverPod = SparkPod.initialPod()
    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_R_MAIN_APP_RESOURCE, mainResource)
    val kubernetesConf = KubernetesConf(
      sparkConf,
      KubernetesDriverSpecificConf(
        Some(RMainAppResource(mainResource)),
        "test-app",
        "r-runner",
        Seq("5", "7", "9")),
      appResourceNamePrefix = "",
      appId = "",
      roleLabels = Map.empty,
      roleAnnotations = Map.empty,
      roleSecretNamesToMountPaths = Map.empty,
      roleSecretEnvNamesToKeyRefs = Map.empty,
      roleEnvs = Map.empty,
      roleVolumes = Seq.empty,
      sparkFiles = Seq.empty[String])

    val step = new RDriverFeatureStep(kubernetesConf)
    val driverContainerwithR = step.configurePod(baseDriverPod).container
    assert(driverContainerwithR.getEnv.size === 2)
    val envs = driverContainerwithR
      .getEnv
      .asScala
      .map(env => (env.getName, env.getValue))
      .toMap
    assert(envs(ENV_R_PRIMARY) === expectedMainResource)
    assert(envs(ENV_R_ARGS) === "5 7 9")
  }
}
