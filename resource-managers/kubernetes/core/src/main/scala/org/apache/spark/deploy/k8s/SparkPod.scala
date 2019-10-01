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

import io.fabric8.kubernetes.api.model.{Container, ContainerBuilder, Pod, PodBuilder}

private[spark] case class SparkPod(pod: Pod, container: Container) {

  /**
   * Convenience method to apply a series of chained transformations to a pod.
   *
   * Use it like:
   *
   *     original.modify { case pod =>
   *       // update pod and return new one
   *     }.modify { case pod =>
   *       // more changes that create a new pod
   *     }.modify {
   *       case pod if someCondition => // new pod
   *     }
   *
   * This makes it cleaner to apply multiple transformations, avoiding having to create
   * a bunch of awkwardly-named local variables. Since the argument is a partial function,
   * it can do matching without needing to exhaust all the possibilities. If the function
   * is not applied, then the original pod will be kept.
   */
  def transform(fn: PartialFunction[SparkPod, SparkPod]): SparkPod = fn.lift(this).getOrElse(this)

}


private[spark] object SparkPod {
  def initialPod(): SparkPod = {
    SparkPod(
      new PodBuilder()
        .withNewMetadata()
        .endMetadata()
        .withNewSpec()
        .endSpec()
        .build(),
      new ContainerBuilder().build())
  }
}
