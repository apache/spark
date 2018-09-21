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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata}

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpecificConf, SparkPod}
import org.apache.spark.deploy.k8s.Config.APP_RESOURCE_TYPE
import org.apache.spark.deploy.k8s.Constants.SPARK_CONF_PATH
import org.apache.spark.deploy.k8s.features.KubernetesFeatureConfigStep
import org.apache.spark.launcher.SparkLauncher

private[spark] class JavaDriverFeatureStep(
  kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf])
  extends KubernetesFeatureConfigStep {
  override def configurePod(pod: SparkPod): SparkPod = {
    val withDriverArgs = new ContainerBuilder(pod.container)
      .addToArgs("driver")
      .addToArgs("--properties-file", SPARK_CONF_PATH)
      .addToArgs("--class", kubernetesConf.roleSpecificConf.mainClass)
      // The user application jar is merged into the spark.jars list and managed through that
      // property, so there is no need to reference it explicitly here.
      .addToArgs(SparkLauncher.NO_RESOURCE)
      .addToArgs(kubernetesConf.roleSpecificConf.appArgs: _*)
      .build()
    SparkPod(pod.pod, withDriverArgs)
  }
  override def getAdditionalPodSystemProperties(): Map[String, String] =
    Map(APP_RESOURCE_TYPE.key -> "java")

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
