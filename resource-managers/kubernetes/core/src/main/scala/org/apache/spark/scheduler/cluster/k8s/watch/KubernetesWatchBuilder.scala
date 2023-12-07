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
package org.apache.spark.scheduler.cluster.k8s.watch

import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.SparkException
import org.apache.spark.deploy.k8s.{Config, KubernetesUtils, KubernetesWatchConf, SparkPod}
import org.apache.spark.deploy.k8s.features.{BasicWatchFeatureStep, EnvSecretsFeatureStep, KubernetesDriverCustomFeatureConfigStep, KubernetesExecutorCustomFeatureConfigStep, KubernetesFeatureConfigStep, KubernetesWatchCustomFeatureConfigStep, LocalDirsFeatureStep, MountSecretsFeatureStep, MountVolumesFeatureStep, WatchKubernetesCredentialsFeatureStep}
import org.apache.spark.util.Utils

private[spark] object KubernetesWatchBuilder {
  /**
   * Build the Kubernetes spec for the watcher from the configuration.
   */
  def buildFromFeatures(
     conf: KubernetesWatchConf,
     kubernetesClient: KubernetesClient): KubernetesWatchSpec = {
    val initialPod = conf.get(Config.KUBERNETES_WATCH_PODTEMPLATE_FILE)
      .map { file =>
        KubernetesUtils.loadPodFromTemplate(
          kubernetesClient,
          file,
          conf.get(Config.KUBERNETES_WATCH_PODTEMPLATE_CONTAINER_NAME),
          conf.sparkConf)
      }
      .getOrElse(SparkPod.initialPod())

    val userRequestedFeatures = conf.get(Config.KUBERNETES_WATCH_POD_FEATURE_STEPS)
      .map { className =>
        val feature = Utils.classForName[Any](className).getConstructor().newInstance()
        val initializedFeature = feature match {
          // Since 3.3, allow user to implement feature with KubernetesDriverConf
          case d: KubernetesWatchCustomFeatureConfigStep =>
            d.init(conf)
            Some(d)
          // raise SparkException with wrong type feature step
          case _: KubernetesExecutorCustomFeatureConfigStep =>
            None
          case _: KubernetesDriverCustomFeatureConfigStep =>
            None
          // Since 3.2, allow user to implement feature without config
          case f: KubernetesFeatureConfigStep =>
            Some(f)
          case _ => None
        }
        initializedFeature.getOrElse {
          throw new SparkException(s"Failed to initialize feature step: $className, " +
            s"please make sure your driver side feature steps are implemented by " +
            s"`${classOf[KubernetesWatchCustomFeatureConfigStep].getName}` or " +
            s"`${classOf[KubernetesFeatureConfigStep].getName}`.")
        }
      }

    val features = Seq(
      new BasicWatchFeatureStep(conf),
      new WatchKubernetesCredentialsFeatureStep(conf),
      new MountSecretsFeatureStep(conf),
      new EnvSecretsFeatureStep(conf),
      new MountVolumesFeatureStep(conf),
      new LocalDirsFeatureStep(conf)
    ) ++ userRequestedFeatures

    val initialSpec = KubernetesWatchSpec(
      pod = initialPod,
      watchKubernetesResources = Seq.empty)

    features.foldLeft(initialSpec) { case (spec, feature) =>
      val newPod = feature.configurePod(spec.pod)
      val addedResources = feature.getAdditionalKubernetesResources()

      KubernetesWatchSpec(
        pod = newPod,
        watchKubernetesResources = spec.watchKubernetesResources ++ addedResources
      )
    }
  }

}
