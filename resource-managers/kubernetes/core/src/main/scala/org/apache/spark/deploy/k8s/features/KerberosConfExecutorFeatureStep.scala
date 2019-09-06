/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
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

import java.io.File

import io.fabric8.kubernetes.api.model.{ContainerBuilder, KeyToPathBuilder, PodBuilder, VolumeBuilder}

import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging

/**
 * Forward kerberos conf file to executor pods iff it has been mounted on driver pod.
 */
private[spark] class KerberosConfExecutorFeatureStep(kubernetesConf: KubernetesExecutorConf)
  extends KubernetesFeatureConfigStep with Logging {

  private val krb5File = new File(s"$KRB_FILE_DIR_PATH/$KRB_FILE_NAME")

  override def configurePod(original: SparkPod): SparkPod = {
    original.transform { case pod if krb5File.exists() =>
      val krb5Volume = new VolumeBuilder()
        .withName(KRB_FILE_VOLUME)
        .withNewConfigMap()
          .withName(s"${kubernetesConf.resourceNamePrefix}-krb5-file")
          .withItems(new KeyToPathBuilder()
            .withKey(krb5File.getName)
            .withPath(krb5File.getName)
            .build())
          .endConfigMap()
        .build()

      val podWithVolume = new PodBuilder(pod.pod)
        .editSpec()
        .addNewVolumeLike(krb5Volume)
          .endVolume()
          .endSpec()
        .build()

      val containerWithMount = new ContainerBuilder(pod.container)
        .addNewVolumeMount()
          .withName(KRB_FILE_VOLUME)
          .withMountPath(krb5File.getAbsolutePath)
          .withSubPath(KRB_FILE_NAME)
          .endVolumeMount()
        .build()
      SparkPod(podWithVolume, containerWithMount)
    }
  }
}
