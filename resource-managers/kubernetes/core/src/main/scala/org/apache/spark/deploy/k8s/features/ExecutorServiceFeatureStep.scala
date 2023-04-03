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

import io.fabric8.kubernetes.api.model.{HasMetadata, ServiceBuilder}

import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, KubernetesUtils, SparkPod}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.{Clock, SystemClock}

private[spark] class ExecutorServiceFeatureStep(
                                                 kubernetesConf: KubernetesExecutorConf,
                                                 clock: Clock = new SystemClock())
  extends KubernetesFeatureConfigStep with Logging {
  import ExecutorServiceFeatureStep._

  private val preferredServiceName = s"${kubernetesConf.resourceNamePrefix}" +
    s"-${kubernetesConf.executorId}$EXECUTOR_SVC_POSTFIX"
  private val resolvedServiceName = if (preferredServiceName.length <= MAX_SERVICE_NAME_LENGTH) {
    preferredServiceName
  } else {
    val randomServiceId = KubernetesUtils.uniqueID(clock = clock)
    val shorterServiceName = s"spark-$randomServiceId$EXECUTOR_SVC_POSTFIX"
    logWarning(s"Executor's hostname would preferably be $preferredServiceName, but this is " +
      s"too long (must be <= $MAX_SERVICE_NAME_LENGTH characters). Falling back to use " +
      s"$shorterServiceName as the executor service's name.")
    shorterServiceName
  }

  private val executorBlockManagerPort = kubernetesConf.sparkConf.getInt(
    config.BLOCK_MANAGER_PORT.key, DEFAULT_BLOCKMANAGER_PORT)


  override def configurePod(pod: SparkPod): SparkPod = {
    val executorHostName = s"$resolvedServiceName.${kubernetesConf.namespace}.svc"
    logInfo(s"Adding service step $executorHostName")
    pod.container.getEnv.forEach { x =>
      if (x.getName == ENV_EXECUTOR_POD_IP) {
        x.setValue(executorHostName)
        x.setValueFrom(null)
      }
    }
    pod
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    val executorService = new ServiceBuilder()
      .withNewMetadata()
      .withName(resolvedServiceName)
      .addToAnnotations(kubernetesConf.serviceAnnotations.asJava)
      .endMetadata()
      .withNewSpec()
      .withClusterIP("None")
      .withSelector(kubernetesConf.labels.asJava)
      .addNewPort()
      .withName(BLOCK_MANAGER_PORT_NAME)
      .withPort(executorBlockManagerPort)
      .withNewTargetPort(executorBlockManagerPort)
      .endPort()
      .endSpec()
      .build()
    Seq(executorService)
  }
}

private[spark] object ExecutorServiceFeatureStep {
  val EXECUTOR_SVC_POSTFIX = "-executor-svc"
  val MAX_SERVICE_NAME_LENGTH = 63
}
