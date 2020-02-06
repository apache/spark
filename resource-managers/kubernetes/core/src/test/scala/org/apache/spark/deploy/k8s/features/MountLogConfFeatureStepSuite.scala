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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.Pod

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{Config, KubernetesTestConf, SparkPod}
import org.apache.spark.launcher.SparkLauncher

class MountLogConfFeatureStepSuite extends SparkFunSuite {

  test("Do not enable, mount logger configuration feature," +
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

  test("Do not mount logger config for client mode," +
    " unless an explicit config map is configured") {

    val sparkConf = new SparkConf(false)
      .set(SparkLauncher.DEPLOY_MODE, "client")
      .set(Config.KUBERNETES_LOGGING_CONF_FILE_NAME, "log4j.properties")
    val conf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val step = new MountLogConfFeatureStep(conf)
    val initPod = SparkPod.initialPod()
    val configuredPod = step.configurePod(initPod)
    assert(configuredPod == initPod, "Pod should be unchanged.")
    val configMapName = "config-map-logging"
    sparkConf.set(Config.KUBERNETES_LOGGING_CONF_CONFIG_MAP, configMapName)
    val conf2 = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val step2 = new MountLogConfFeatureStep(conf2)
    assert(step2.getAdditionalPodSystemProperties().nonEmpty)
    val configuredPod2 = step2.configurePod(initPod)
    assert(configuredPod2.pod.getSpec.getVolumes.get(0).getConfigMap.getName == configMapName)
  }

  test("Do not auto-create a ConfigMap if user provided ConfigMap is configured.") {
    val sparkConf = new SparkConf(false)
      .set(SparkLauncher.DEPLOY_MODE, "cluster")
      .set(Config.KUBERNETES_LOGGING_CONF_CONFIG_MAP, "user-created-config-map-name")
    val conf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val step = new MountLogConfFeatureStep(conf)
    assert(step.getAdditionalKubernetesResources() == Seq.empty)
  }

  test("User provided ConfigMap takes precedence and is configured properly.") {
    val configMapName = "user-created-config-map-name"
    val sparkConf = new SparkConf(false)
      .set(SparkLauncher.DEPLOY_MODE, "cluster")
      .set(Config.KUBERNETES_LOGGING_CONF_CONFIG_MAP, configMapName)
    val conf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val step = new MountLogConfFeatureStep(conf)
    val configuredPod = step.configurePod(SparkPod.initialPod())
    assert(hasConfigMap(configuredPod.pod, configMapName))
  }

  private def hasConfigMap(pod: Pod, configMapName: String): Boolean = {
    pod.getSpec.getVolumes.asScala.exists { volume =>
      volume.getConfigMap.getName == configMapName
    }
  }
}
