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

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, HasMetadata}

import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}

private[spark] class EnvSecretsFeatureStep(kubernetesConf: KubernetesConf)
  extends KubernetesFeatureConfigStep {
  override def configurePod(pod: SparkPod): SparkPod = {
    val addedEnvSecrets = kubernetesConf
      .secretEnvNamesToKeyRefs
      .map{ case (envName, keyRef) =>
        // Keyref parts
        val keyRefParts = keyRef.split(":")
        require(keyRefParts.size == 2, "SecretKeyRef must be in the form name:key.")
        val name = keyRefParts(0)
        val key = keyRefParts(1)
        new EnvVarBuilder()
          .withName(envName)
          .withNewValueFrom()
            .withNewSecretKeyRef()
              .withKey(key)
              .withName(name)
            .endSecretKeyRef()
          .endValueFrom()
          .build()
      }

    val containerWithEnvVars = new ContainerBuilder(pod.container)
      .addAllToEnv(addedEnvSecrets.toSeq.asJava)
      .build()
    SparkPod(pod.pod, containerWithEnvVars)
  }
}
