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

import io.fabric8.kubernetes.api.model.HasMetadata

import org.apache.spark.deploy.k8s.SparkPod

/**
 * A collection of functions that together represent a "feature" in pods that are launched for
 * Spark drivers and executors.
 */
private[spark] trait KubernetesFeatureConfigStep {

  /**
   * Apply modifications on the given pod in accordance to this feature. This can include attaching
   * volumes, adding environment variables, and adding labels/annotations.
   * <p>
   * Note that we should return a SparkPod that keeps all of the properties of the passed SparkPod
   * object. So this is correct:
   * <pre>
   * {@code val configuredPod = new PodBuilder(pod.pod)
   *     .editSpec()
   *     ...
   *     .build()
   *   val configuredContainer = new ContainerBuilder(pod.container)
   *     ...
   *     .build()
   *   SparkPod(configuredPod, configuredContainer)
   *  }
   * </pre>
   * This is incorrect:
   * <pre>
   * {@code val configuredPod = new PodBuilder() // Loses the original state
   *     .editSpec()
   *     ...
   *     .build()
   *   val configuredContainer = new ContainerBuilder() // Loses the original state
   *     ...
   *     .build()
   *   SparkPod(configuredPod, configuredContainer)
   *  }
   * </pre>
   */
  def configurePod(pod: SparkPod): SparkPod

  /**
   * Return any system properties that should be set on the JVM in accordance to this feature.
   */
  def getAdditionalPodSystemProperties(): Map[String, String] = Map.empty

  /**
   * Return any additional Kubernetes resources that should be added to support this feature. Only
   * applicable when creating the driver in cluster mode.
   */
  def getAdditionalKubernetesResources(): Seq[HasMetadata] = Seq.empty
}
