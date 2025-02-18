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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._

class PriorityClassFeatureStepSuite extends SparkFunSuite {

  test("Test driver priority class name is set") {
    val sparkConf = new SparkConf(false).set(KUBERNETES_PRIORITY_CLASS_NAME, "high-criticality-job")
    val kubernetesDriverConf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val priorityClassFeatureStep = new PriorityClassFeatureStep(kubernetesDriverConf)
    val driverPod = priorityClassFeatureStep.configurePod(SparkPod.initialPod())
    assert(driverPod.pod.getSpec.getPriorityClassName === "high-criticality-job")
  }

  test("Test executor priority class name is set") {
    val sparkConf = new SparkConf(false).set(KUBERNETES_PRIORITY_CLASS_NAME, "high-criticality-job")
    val kubernetesExecutorConf = KubernetesTestConf.createExecutorConf(sparkConf = sparkConf)
    val priorityClassFeatureStep = new PriorityClassFeatureStep(kubernetesExecutorConf)
    val executorPod = priorityClassFeatureStep.configurePod(SparkPod.initialPod())
    assert(executorPod.pod.getSpec.getPriorityClassName === "high-criticality-job")
  }

  test("Test pod spec remain unchanged when no priority class name is set") {
    val sparkConf = new SparkConf(false)
    val kubernetesExecutorConf = KubernetesTestConf.createExecutorConf(sparkConf = sparkConf)
    val executorStep = new PriorityClassFeatureStep(kubernetesExecutorConf)
    val executorPod = executorStep.configurePod(SparkPod.initialPod())
    val podWithoutStep = SparkPod.initialPod()
    assert(executorPod.pod.getSpec.getPriorityClassName === null)
    assert(executorPod.pod.getSpec === podWithoutStep.pod.getSpec)
  }
}
