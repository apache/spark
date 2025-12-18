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

import io.fabric8.kubernetes.api.model.PodSpec
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._

class ExecutorKubernetesCredentialsFeatureStepSuite extends SparkFunSuite with BeforeAndAfter {

  private var baseConf: SparkConf = _

  before {
    baseConf = new SparkConf(false)
  }

  test("configure spark pod with executor service account") {
    baseConf.set(KUBERNETES_EXECUTOR_SERVICE_ACCOUNT_NAME, "executor-name")
    val spec = evaluateStep()
    assertSAName("executor-name", spec)
  }

  test("configure spark pod with with driver service account " +
    "and without executor service account") {
    baseConf.set(KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME, "driver-name")
    val spec = evaluateStep()
    assertSAName("driver-name", spec)
  }

  test("configure spark pod with with driver service account " +
    "and with executor service account") {
    baseConf.set(KUBERNETES_DRIVER_SERVICE_ACCOUNT_NAME, "driver-name")
    baseConf.set(KUBERNETES_EXECUTOR_SERVICE_ACCOUNT_NAME, "executor-name")
    val spec = evaluateStep()
    assertSAName("executor-name", spec)
  }

  private def assertSAName(expectedServiceAccountName: String,
      spec: PodSpec): Unit = {
    assert(spec.getServiceAccountName.equals(expectedServiceAccountName))
    assert(spec.getServiceAccount.equals(expectedServiceAccountName))
  }

  private def evaluateStep(): PodSpec = {
    val executorConf = KubernetesTestConf.createExecutorConf(
        sparkConf = baseConf)
    val step = new ExecutorKubernetesCredentialsFeatureStep(executorConf)
    step
      .configurePod(SparkPod.initialPod())
      .pod
      .getSpec
  }
}
