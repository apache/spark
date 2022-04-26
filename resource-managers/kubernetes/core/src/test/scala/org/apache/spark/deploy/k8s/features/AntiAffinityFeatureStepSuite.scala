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
import org.apache.spark.deploy.k8s.{KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants.SPARK_APP_ID_LABEL

class AntiAffinityFeatureStepSuite extends SparkFunSuite {

  test("enable executor pod antiAffinity") {
    val appId = "test-executor-pod-antiAffinity"
    val conf = new SparkConf()
      .set("spark.app.id", appId)
      .set("spark.kubernetes.executor.pod.antiAffinity.enable", "true")
    val executorConf = KubernetesTestConf.createExecutorConf(conf)
    val sparkPod =
      new AntiAffinityFeatureStep(executorConf).configurePod(SparkPod.initialPod())

    assert(sparkPod.pod.getSpec.getAffinity.getPodAntiAffinity != null)
    assert(sparkPod.pod.getSpec.getAffinity.getPodAntiAffinity
      .getPreferredDuringSchedulingIgnoredDuringExecution != null)
    assert(sparkPod.pod.getSpec.getAffinity.getPodAntiAffinity
      .getPreferredDuringSchedulingIgnoredDuringExecution.size() == 1)
    assert(sparkPod.pod.getSpec.getAffinity.getPodAntiAffinity
      .getPreferredDuringSchedulingIgnoredDuringExecution.get(0).getPodAffinityTerm != null)
    assert(sparkPod.pod.getSpec.getAffinity.getPodAntiAffinity
      .getPreferredDuringSchedulingIgnoredDuringExecution.get(0)
      .getPodAffinityTerm.getLabelSelector != null)
    assert(sparkPod.pod.getSpec.getAffinity.getPodAntiAffinity
      .getPreferredDuringSchedulingIgnoredDuringExecution.get(0)
      .getPodAffinityTerm.getLabelSelector.getMatchExpressions != null)
    assert(sparkPod.pod.getSpec.getAffinity.getPodAntiAffinity
      .getPreferredDuringSchedulingIgnoredDuringExecution.get(0)
      .getPodAffinityTerm.getLabelSelector.getMatchExpressions.size() == 1)
    assert(sparkPod.pod.getSpec.getAffinity.getPodAntiAffinity
      .getPreferredDuringSchedulingIgnoredDuringExecution.get(0)
      .getPodAffinityTerm.getLabelSelector.getMatchExpressions.get(0)
      .getKey.equals(SPARK_APP_ID_LABEL))
    assert(sparkPod.pod.getSpec.getAffinity.getPodAntiAffinity
      .getPreferredDuringSchedulingIgnoredDuringExecution.get(0)
      .getPodAffinityTerm.getLabelSelector.getMatchExpressions.get(0)
      .getValues.get(0).equals(appId))
  }
}
