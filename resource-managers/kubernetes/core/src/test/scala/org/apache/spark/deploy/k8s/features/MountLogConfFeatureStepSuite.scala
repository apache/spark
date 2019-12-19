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

package org.apache.spark.deploy.k8s.features

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{Config, KubernetesTestConf, SparkPod}
import org.apache.spark.launcher.SparkLauncher

class MountLogConfFeatureStepSuite extends SparkFunSuite {
  // TODO add more tests.
  test("Do not enable mount logging conf feature," +
    " if neither of logging configuration file or user defined config map is configured.") {
    val sparkConf = new SparkConf(false)
      .set(SparkLauncher.DEPLOY_MODE, "cluster")
      .set(Config.KUBERNETES_LOGGING_CONF_FILE_NAME, "some-non-existent-file-name.none")
    val conf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val step = new MountLogConfFeatureStep(conf)
    val initPod = SparkPod.initialPod()
    val configuredPod = step.configurePod(initPod)
    assert(configuredPod == initPod, "Pod should be unchanged.")
  }

  test("Do not create a config map if user provided configMap is configured.") {
    val sparkConf = new SparkConf(false)
      .set(SparkLauncher.DEPLOY_MODE, "cluster")
      .set(Config.KUBERNETES_LOGGING_CONF_CONFIG_MAP, "user-created-config-map-name")
    val conf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val step = new MountLogConfFeatureStep(conf)
    assert(step.getAdditionalKubernetesResources() == Seq.empty)
  }
}
