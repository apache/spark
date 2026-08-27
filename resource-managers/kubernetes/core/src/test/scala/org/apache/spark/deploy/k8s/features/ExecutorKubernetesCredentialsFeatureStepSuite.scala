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

import io.fabric8.kubernetes.api.model.{PodBuilder, PodSpec}
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

  test("SPARK-58910: keep the service account named by the executor pod template") {
    baseConf.set(KUBERNETES_EXECUTOR_SERVICE_ACCOUNT_NAME, "executor-name")
    // Either spelling means the template already picked an account, so the configured one must not
    // replace it.
    Seq(
      (podWithAccount(serviceAccountName = Some("template-name")), "template-name", null),
      (podWithAccount(serviceAccount = Some("template-name")), null, "template-name")
    ).foreach { case (templatePod, expectedName, expectedAlias) =>
      val spec = evaluateStep(templatePod)
      assert(spec.getServiceAccountName === expectedName)
      assert(spec.getServiceAccount === expectedAlias)
    }
  }

  test("SPARK-58910: an empty service account name in the template counts as unset") {
    baseConf.set(KUBERNETES_EXECUTOR_SERVICE_ACCOUNT_NAME, "executor-name")
    // SetDefaults_PodSpec keys off the name being empty rather than null, and an empty alias copies
    // up as an empty name, so neither leaves the pod with an account.
    Seq(
      podWithAccount(serviceAccountName = Some("")),
      podWithAccount(serviceAccount = Some(""))
    ).foreach(templatePod => assertSAName("executor-name", evaluateStep(templatePod)))
  }

  private def assertSAName(expectedServiceAccountName: String,
      spec: PodSpec): Unit = {
    assert(spec.getServiceAccountName.equals(expectedServiceAccountName))
    assert(spec.getServiceAccount.equals(expectedServiceAccountName))
  }

  /**
   * An executor pod whose spec names a service account, standing in for a user pod template. A
   * template is parsed with no API-server defaulting, so it can name either field on its own.
   */
  private def podWithAccount(
      serviceAccount: Option[String] = None,
      serviceAccountName: Option[String] = None): SparkPod = {
    val basePod = SparkPod.initialPod()
    val spec = new PodBuilder(basePod.pod).editOrNewSpec()
    serviceAccount.foreach(spec.withServiceAccount(_))
    serviceAccountName.foreach(spec.withServiceAccountName(_))
    SparkPod(spec.endSpec().build(), basePod.container)
  }

  private def evaluateStep(pod: SparkPod = SparkPod.initialPod()): PodSpec = {
    val executorConf = KubernetesTestConf.createExecutorConf(
        sparkConf = baseConf)
    val step = new ExecutorKubernetesCredentialsFeatureStep(executorConf)
    step
      .configurePod(pod)
      .pod
      .getSpec
  }
}
