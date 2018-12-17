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

import io.fabric8.kubernetes.api.model.{Container, ContainerBuilder, HasMetadata, Pod, PodBuilder}

import org.apache.spark.SparkConf

/**
 * Represents the components and characteristics of a Spark driver. The driver can be considered
 * as being comprised of the driver pod itself, any other Kubernetes resources that the driver
 * pod depends on, and the SparkConf that should be supplied to the Spark application. The driver
 * container should be operated on via the specific field of this case class as opposed to trying
 * to edit the container directly on the pod. The driver container should be attached at the
 * end of executing all submission steps.
 */
private[spark] case class KubernetesDriverSpec(
    driverPod: Pod,
    driverContainer: Container,
    otherKubernetesResources: Seq[HasMetadata],
    driverSparkConf: SparkConf)

private[spark] object KubernetesDriverSpec {
  def initialSpec(initialSparkConf: SparkConf): KubernetesDriverSpec = {
    KubernetesDriverSpec(
      // Set new metadata and a new spec so that submission steps can use
      // PodBuilder#editMetadata() and/or PodBuilder#editSpec() safely.
      new PodBuilder().withNewMetadata().endMetadata().withNewSpec().endSpec().build(),
      new ContainerBuilder().build(),
      Seq.empty[HasMetadata],
      initialSparkConf.clone())
  }
}
