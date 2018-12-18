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

import java.io.File

import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.SecurityManager
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.features._

private[spark] class KubernetesExecutorBuilder {

  def buildFromFeatures(
      conf: KubernetesExecutorConf,
      secMgr: SecurityManager,
      client: KubernetesClient): SparkPod = {
    val initialPod = conf.get(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE)
      .map { file =>
        KubernetesUtils.loadPodFromTemplate(
          client,
          new File(file),
          conf.get(Config.KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME))
      }
      .getOrElse(SparkPod.initialPod())

    val features = Seq(
      new BasicExecutorFeatureStep(conf, secMgr),
      new MountSecretsFeatureStep(conf),
      new EnvSecretsFeatureStep(conf),
      new LocalDirsFeatureStep(conf),
      new MountVolumesFeatureStep(conf))

    features.foldLeft(initialPod) { case (pod, feature) => feature.configurePod(pod) }
  }

}
