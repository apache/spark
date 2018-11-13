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

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers.{eq => Eq}
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesExecutorSpecificConf, SparkPod}
import org.apache.spark.deploy.k8s.Constants.KERBEROS_SPARK_USER_NAME
import org.apache.spark.deploy.k8s.features.hadooputils.HadoopBootstrapUtil

class HadoopSparkUserExecutorFeatureStepSuite extends SparkFunSuite with BeforeAndAfter {
  private val hadoopSparkUser = "SPARK_USER_NAME"
  private val sparkPod = SparkPod.initialPod()

  @Mock
  private var kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf] = _

  @Mock
  private var hadoopBootstrapUtil: HadoopBootstrapUtil = _

  before {
    MockitoAnnotations.initMocks(this)
    val sparkConf = new SparkConf(false)
      .set(KERBEROS_SPARK_USER_NAME, hadoopSparkUser)
    when(kubernetesConf.sparkConf).thenReturn(sparkConf)
  }

  test("bootstrapSparkUserPod being applied to a base spark pod") {
    when(hadoopBootstrapUtil.bootstrapSparkUserPod(
      Eq(hadoopSparkUser),
      Eq(sparkPod)
    )).thenAnswer(new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock) : SparkPod = {
        KubernetesFeaturesTestUtils.userBootPod(invocation.getArgumentAt(1, classOf[SparkPod]))
      }
    })
    val sUserStep = new HadoopSparkUserExecutorFeatureStep(kubernetesConf, hadoopBootstrapUtil)
    val pod = sUserStep.configurePod(sparkPod)
    KubernetesFeaturesTestUtils.podHasLabels(pod.pod, Map("bootstrap-user" -> "true"))
    assert(sUserStep.getAdditionalPodSystemProperties().isEmpty)
    assert(sUserStep.getAdditionalKubernetesResources().isEmpty)
  }
}
