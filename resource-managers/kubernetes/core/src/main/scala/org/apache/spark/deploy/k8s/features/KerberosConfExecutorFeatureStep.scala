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

import io.fabric8.kubernetes.api.model._

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._

/**
 * Mounts the krb5 config map on the executor pod.
 */
private[spark] class KerberosConfExecutorFeatureStep(conf: KubernetesConf)
  extends KubernetesFeatureConfigStep {

  private def krb5FileMapName: Option[String] = conf.getOption(KRB_FILE_MAP_NAME)

  override def configurePod(original: SparkPod): SparkPod = {
    original.transform { case pod if krb5FileMapName.isDefined =>
      val configMapVolume = new VolumeBuilder()
        .withName(KRB_FILE_VOLUME)
        .withNewConfigMap()
          .withName(krb5FileMapName.get)
          .endConfigMap()
        .build()

      val podWithVolume = new PodBuilder(pod.pod)
        .editSpec()
          .addNewVolumeLike(configMapVolume)
            .endVolume()
          .endSpec()
        .build()

      val containerWithMount = new ContainerBuilder(pod.container)
        .addNewVolumeMount()
          .withName(KRB_FILE_VOLUME)
          .withMountPath(KRB_FILE_DIR_PATH + "/krb5.conf")
          .withSubPath("krb5.conf")
          .endVolumeMount()
        .build()

      SparkPod(podWithVolume, containerWithMount)
    }
  }
}
