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
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.features.hadooputils.HadoopBootstrapUtil

class KerberosConfExecutorFeatureStepSuite extends SparkFunSuite with BeforeAndAfter {
  private val sparkPod = SparkPod.initialPod()

  @Mock
  private var kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf] = _

  @Mock
  private var hadoopBootstrapUtil: HadoopBootstrapUtil = _

  before {
    MockitoAnnotations.initMocks(this)
    when(kubernetesConf.hadoopBootstrapUtil).thenReturn(hadoopBootstrapUtil)
  }

  test("Testing bootstrapKerberosPod") {
    val krbConfName = "KRB_CONF_NAME"
    val dtSecretName = "DT_SECRET_NAME"
    val dtSecretKey = "DT_SECRET_KEY"
    val dtUser = "DT_USER"
    val sparkConf = new SparkConf(false)
      .set(KRB5_CONFIG_MAP_NAME, krbConfName)
      .set(KERBEROS_DT_SECRET_NAME, dtSecretName)
      .set(KERBEROS_DT_SECRET_KEY, dtSecretKey)
      .set(KERBEROS_SPARK_USER_NAME, dtUser)
    when(kubernetesConf.sparkConf).thenReturn(sparkConf)
    when(hadoopBootstrapUtil.bootstrapKerberosPod(
      Eq(dtSecretName),
      Eq(dtSecretKey),
      Eq(dtUser),
      Eq(None),
      Eq(None),
      Eq(Some(krbConfName)),
      Eq(sparkPod)
    )).thenAnswer(new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock) : SparkPod = {
        KubernetesFeaturesTestUtils.krbBootPod(invocation.getArgumentAt(6, classOf[SparkPod]))
      }
    })
    val kConfStep = new KerberosConfExecutorFeatureStep(kubernetesConf)
    val pod = kConfStep.configurePod(sparkPod)
    assert(KubernetesFeaturesTestUtils.podHasLabels(pod.pod, Map("bootstrap-kerberos" -> "true")))
    assert(kConfStep.getAdditionalPodSystemProperties() === Map.empty)
    assert(kConfStep.getAdditionalKubernetesResources() === Seq.empty)
  }
}
