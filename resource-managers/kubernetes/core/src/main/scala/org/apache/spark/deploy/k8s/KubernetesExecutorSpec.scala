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
package org.apache.spark.deploy.k8s

import java.util.{List => JList}

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.HasMetadata

import org.apache.spark.annotation.{DeveloperApi, Since, Stable}

/**
 * :: DeveloperApi ::
 *
 * Spec for executor pod and resources, used for K8s operations internally
 * and Spark K8s operator.
 */
@Stable
@DeveloperApi
@Since("4.4.0")
case class KubernetesExecutorSpec(
    pod: SparkPod,
    executorKubernetesResources: Seq[HasMetadata]) {

  /** Get executor Kubernetes resources as a Java-friendly list. */
  @Since("4.4.0")
  def getExecutorKubernetesResourcesAsJavaList: JList[HasMetadata] =
    executorKubernetesResources.asJava
}

@Stable
@DeveloperApi
@Since("4.4.0")
object KubernetesExecutorSpec {
  @Since("4.4.0")
  def create(
      pod: SparkPod,
      executorKubernetesResources: JList[HasMetadata]): KubernetesExecutorSpec = {
    KubernetesExecutorSpec(
      pod,
      executorKubernetesResources.asScala.toSeq)
  }
}
