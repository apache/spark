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
package org.apache.spark.deploy.k8s.submit

import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.features._
import org.apache.spark.util.Utils

private[spark] class KubernetesDriverBuilder {

  def buildFromFeatures(
      conf: KubernetesDriverConf,
      client: KubernetesClient): KubernetesDriverSpec = {
    val initialPod = conf.get(Config.KUBERNETES_DRIVER_PODTEMPLATE_FILE)
      .map { file =>
        KubernetesUtils.loadPodFromTemplate(
          client,
          file,
          conf.get(Config.KUBERNETES_DRIVER_PODTEMPLATE_CONTAINER_NAME),
          conf.sparkConf)
      }
      .getOrElse(SparkPod.initialPod())

    val userFeatures = conf.get(Config.KUBERNETES_DRIVER_POD_FEATURE_STEPS)
      .map { className =>
        Utils.classForName(className).newInstance().asInstanceOf[KubernetesFeatureConfigStep]
      }

    val features = Seq(
      new BasicDriverFeatureStep(conf),
      new DriverKubernetesCredentialsFeatureStep(conf),
      new DriverServiceFeatureStep(conf),
      new MountSecretsFeatureStep(conf),
      new EnvSecretsFeatureStep(conf),
      new MountVolumesFeatureStep(conf),
      new DriverCommandFeatureStep(conf),
      new HadoopConfDriverFeatureStep(conf),
      new KerberosConfDriverFeatureStep(conf),
      new PodTemplateConfigMapStep(conf),
      new LocalDirsFeatureStep(conf)) ++ userFeatures

    val spec = KubernetesDriverSpec(
      initialPod,
      driverPreKubernetesResources = Seq.empty,
      driverKubernetesResources = Seq.empty,
      conf.sparkConf.getAll.toMap)

    features.foldLeft(spec) { case (spec, feature) =>
      val configuredPod = feature.configurePod(spec.pod)
      val addedSystemProperties = feature.getAdditionalPodSystemProperties()
      val addedPreResources = feature.getAdditionalPreKubernetesResources()
      val addedResources = feature.getAdditionalKubernetesResources()
      KubernetesDriverSpec(
        configuredPod,
        spec.driverPreKubernetesResources ++ addedPreResources,
        spec.driverKubernetesResources ++ addedResources,
        spec.systemProperties ++ addedSystemProperties)
    }
  }

}
