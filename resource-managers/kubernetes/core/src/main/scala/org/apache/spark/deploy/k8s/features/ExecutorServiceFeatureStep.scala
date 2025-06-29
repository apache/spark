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

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata, ServiceBuilder}

import org.apache.spark.SparkException
import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, SparkPod}
import org.apache.spark.internal.config.{BLOCK_MANAGER_PORT, SHUFFLE_SERVICE_PORT}

class ExecutorServiceFeatureStep(conf: KubernetesExecutorConf) extends KubernetesFeatureConfigStep {
  private val spark_app_selector_label = "spark-app-selector"
  private val spark_exec_id_label = "spark-exec-id"
  private val service_selector_labels = Set(spark_app_selector_label, spark_exec_id_label)
  private lazy val selector = conf.labels
    .filter { case (key, _) => service_selector_labels.contains(key) }

  private lazy val sparkAppSelector = getLabel(spark_app_selector_label)
  private lazy val sparkExecId = getLabel(spark_exec_id_label)
  // name length is 8 + 38 + 6 + 10 = 62
  // which fits in KUBERNETES_DNS_LABEL_NAME_MAX_LENGTH = 63
  private lazy val serviceName = s"svc-$sparkAppSelector-exec-$sparkExecId"

  // The executor kubernetes services requires BLOCK_MANAGER_PORT to be set
  private val blockManagerPortName = "spark-block-manager"
  private val blockManagerPort = conf.sparkConf.get(BLOCK_MANAGER_PORT)
  SparkException.require(blockManagerPort > 0,
    "EXECUTOR_KUBERNETES_SERVICE_REQUIRES_BLOCK_MANAGER_PORT",
    Map(
      "blockManagerPortConfigKey" -> BLOCK_MANAGER_PORT.key,
      "defaultShuffleServicePort" -> SHUFFLE_SERVICE_PORT.defaultValue.get.toString));

  private def getLabel(label: String): String = {
    val value = conf.labels.get(label)
    value.getOrElse(
      throw new SparkException(s"This feature step requires label $label")
    )
  }

  override def configurePod(pod: SparkPod): SparkPod = {
    SparkPod(
      pod.pod,
      // tell the executor entry point its Kubernetes service name
      new ContainerBuilder(pod.container)
        .addNewEnv()
        .withName("EXECUTOR_SERVICE_NAME")
        .withValue(serviceName)
        .endEnv()
        .build())
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    val service = new ServiceBuilder()
      .withNewMetadata()
      .withName(serviceName)
      .endMetadata()
      .withNewSpec()
      .withSelector(selector.asJava)
      .addNewPort()
      .withName(blockManagerPortName)
      .withPort(blockManagerPort)
      .withNewTargetPort(blockManagerPort)
      .endPort()
      .endSpec()
      .build()

    Seq(service)
  }
}
