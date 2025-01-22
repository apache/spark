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

import org.apache.spark.SparkException
import org.apache.spark.annotation.{DeveloperApi, Since, Unstable}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.features._
import org.apache.spark.util.Utils

/**
 * ::DeveloperApi::
 *
 * KubernetesDriverBuilder builds k8s spec for driver, used for K8s operations internally
 * and Spark K8s operator.
 */
@Unstable
@DeveloperApi
class KubernetesDriverBuilder {

  @Since("3.0.0")
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
        val feature = Utils.classForName[Any](className).getConstructor().newInstance()
        val initializedFeature = feature match {
          // Since 3.3, allow user to implement feature with KubernetesDriverConf
          case d: KubernetesDriverCustomFeatureConfigStep =>
            d.init(conf)
            Some(d)
          // raise SparkException with wrong type feature step
          case _: KubernetesExecutorCustomFeatureConfigStep =>
            None
          // Since 3.2, allow user to implement feature without config
          case f: KubernetesFeatureConfigStep =>
            Some(f)
          case _ => None
        }
        initializedFeature.getOrElse {
          throw new SparkException(s"Failed to initialize feature step: $className, " +
            s"please make sure your driver side feature steps are implemented by " +
            s"`${classOf[KubernetesDriverCustomFeatureConfigStep].getName}` or " +
            s"`${classOf[KubernetesFeatureConfigStep].getName}`.")
        }
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
