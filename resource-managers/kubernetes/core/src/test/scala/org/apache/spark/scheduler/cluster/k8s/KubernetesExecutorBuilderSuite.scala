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
package org.apache.spark.scheduler.cluster.k8s

import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.features.KubernetesExecutorCustomFeatureConfigStep
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.resource.ResourceProfile

class KubernetesExecutorBuilderSuite extends PodBuilderSuite {
  val POD_ROLE: String = "executor"
  val TEST_ANNOTATION_KEY: String = "executor-annotation-key"
  val TEST_ANNOTATION_VALUE: String = "executor-annotation-value"

  override protected def templateFileConf: ConfigEntry[_] = {
    Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE
  }

  override protected def userFeatureStepsConf: ConfigEntry[_] = {
    Config.KUBERNETES_EXECUTOR_POD_FEATURE_STEPS
  }

  override protected def userFeatureStepWithExpectedAnnotation: (String, String) = {
    ("org.apache.spark.scheduler.cluster.k8s.TestStepWithExecConf", TEST_ANNOTATION_VALUE)
  }

  override protected def wrongTypeFeatureStep: String = {
    "org.apache.spark.deploy.k8s.submit.TestStepWithDrvConf"
  }

  override protected def buildPod(sparkConf: SparkConf, client: KubernetesClient): SparkPod = {
    sparkConf.set("spark.driver.host", "https://driver.host.com")
    val conf = KubernetesTestConf.createExecutorConf(sparkConf = sparkConf)
    val secMgr = new SecurityManager(sparkConf)
    val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
    new KubernetesExecutorBuilder().buildFromFeatures(conf, secMgr, client, defaultProfile).pod
  }
}

/**
 * A test executor user feature step would be used in only executor.
 */
class TestStepWithExecConf extends KubernetesExecutorCustomFeatureConfigStep {
  import io.fabric8.kubernetes.api.model._

  private var executorConf: KubernetesExecutorConf = _

  def init(config: KubernetesExecutorConf): Unit = {
    executorConf = config
  }

  override def configurePod(pod: SparkPod): SparkPod = {
    val k8sPodBuilder = new PodBuilder(pod.pod)
      .editOrNewMetadata()
       // The annotation key = TEST_ANNOTATION_KEY, value = TEST_ANNOTATION_VALUE
      .addToAnnotations("executor-annotation-key", executorConf.get("executor-annotation-key"))
      .endMetadata()
    val k8sPod = k8sPodBuilder.build()
    SparkPod(k8sPod, pod.container)
  }
}
