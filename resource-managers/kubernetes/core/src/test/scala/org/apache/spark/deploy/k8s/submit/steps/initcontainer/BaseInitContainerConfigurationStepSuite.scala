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
package org.apache.spark.deploy.k8s.submit.steps.initcontainer

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.{InitContainerBootstrap, PodWithDetachedInitContainer}
import org.apache.spark.deploy.k8s.Config._

class BaseInitContainerConfigurationStepSuite extends SparkFunSuite with BeforeAndAfter {

  private val SPARK_JARS = Seq(
    "hdfs://localhost:9000/app/jars/jar1.jar", "file:///app/jars/jar2.jar")
  private val SPARK_FILES = Seq(
    "hdfs://localhost:9000/app/files/file1.txt", "file:///app/files/file2.txt")
  private val JARS_DOWNLOAD_PATH = "/var/data/jars"
  private val FILES_DOWNLOAD_PATH = "/var/data/files"
  private val POD_LABEL = Map("bootstrap" -> "true")
  private val INIT_CONTAINER_NAME = "init-container"
  private val DRIVER_CONTAINER_NAME = "driver-container"

  @Mock
  private var podAndInitContainerBootstrap : InitContainerBootstrap = _

  before {
    MockitoAnnotations.initMocks(this)
    when(podAndInitContainerBootstrap.bootstrapInitContainer(
      any[PodWithDetachedInitContainer])).thenAnswer(new Answer[PodWithDetachedInitContainer] {
      override def answer(invocation: InvocationOnMock) : PodWithDetachedInitContainer = {
        val pod = invocation.getArgumentAt(0, classOf[PodWithDetachedInitContainer])
        pod.copy(
          pod = new PodBuilder(pod.pod)
            .withNewMetadata()
            .addToLabels("bootstrap", "true")
            .endMetadata()
            .withNewSpec().endSpec()
            .build(),
          initContainer = new ContainerBuilder()
            .withName(INIT_CONTAINER_NAME)
            .build(),
          mainContainer = new ContainerBuilder()
            .withName(DRIVER_CONTAINER_NAME)
            .build()
        )}})
  }

  test("Test of additionalDriverSparkConf with mix of remote files and jars") {
    val baseInitStep = new BaseInitContainerConfigurationStep(
      SPARK_JARS,
      SPARK_FILES,
      JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_PATH,
      podAndInitContainerBootstrap)
    val expectedDriverSparkConf = Map(
      JARS_DOWNLOAD_LOCATION.key -> JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_LOCATION.key -> FILES_DOWNLOAD_PATH,
      INIT_CONTAINER_REMOTE_JARS.key -> "hdfs://localhost:9000/app/jars/jar1.jar",
      INIT_CONTAINER_REMOTE_FILES.key -> "hdfs://localhost:9000/app/files/file1.txt")
    val initContainerSpec = InitContainerSpec(
      Map.empty[String, String],
      Map.empty[String, String],
      new Container(),
      new Container(),
      new Pod,
      Seq.empty[HasMetadata])
    val returnContainerSpec = baseInitStep.configureInitContainer(initContainerSpec)
    assert(expectedDriverSparkConf === returnContainerSpec.properties)
    assert(returnContainerSpec.initContainer.getName == INIT_CONTAINER_NAME)
    assert(returnContainerSpec.driverContainer.getName == DRIVER_CONTAINER_NAME)
    assert(returnContainerSpec.driverPod.getMetadata.getLabels.asScala === POD_LABEL)
  }
}
