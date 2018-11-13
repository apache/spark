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
import org.apache.spark.deploy.k8s.Constants.HADOOP_CONFIG_MAP_NAME
import org.apache.spark.deploy.k8s.features.hadooputils.HadoopBootstrapUtil

class HadoopConfExecutorFeatureStepSuite extends SparkFunSuite with BeforeAndAfter {
  private val hadoopConfMapName = "HADOOP_CONF_NAME"
  private val sparkPod = SparkPod.initialPod()

  @Mock
  private var kubernetesConf: KubernetesConf[KubernetesExecutorSpecificConf] = _

  @Mock
  private var hadoopBootstrapUtil: HadoopBootstrapUtil = _

  before {
    MockitoAnnotations.initMocks(this)
    val sparkConf = new SparkConf(false)
      .set(HADOOP_CONFIG_MAP_NAME, hadoopConfMapName)
    when(kubernetesConf.sparkConf).thenReturn(sparkConf)
  }

  test("bootstrapHadoopConf being applied to a base spark pod") {
    when(hadoopBootstrapUtil.bootstrapHadoopConfDir(
      Eq(None),
      Eq(None),
      Eq(Some(hadoopConfMapName)),
      Eq(sparkPod)
    )).thenAnswer(new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock) : SparkPod = {
        KubernetesFeaturesTestUtils.hadoopConfBootPod(
          invocation.getArgumentAt(3, classOf[SparkPod]))
      }
    })
    val hConfStep = new HadoopConfExecutorFeatureStep(kubernetesConf, hadoopBootstrapUtil)
    val pod = hConfStep.configurePod(sparkPod)
    KubernetesFeaturesTestUtils.podHasLabels(pod.pod, Map("bootstrap-hconf" -> "true"))
    assert(hConfStep.getAdditionalPodSystemProperties().isEmpty)
    assert(hConfStep.getAdditionalKubernetesResources().isEmpty)
  }
}
