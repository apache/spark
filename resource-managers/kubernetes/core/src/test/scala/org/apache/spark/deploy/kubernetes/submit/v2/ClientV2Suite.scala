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
package org.apache.spark.deploy.kubernetes.submit.v2

import java.io.File

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, DoneablePod, HasMetadata, Pod, PodBuilder, PodList, Secret, SecretBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.{MixedOperation, NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable, PodResource}
import org.hamcrest.{BaseMatcher, Description}
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Matchers.{any, anyVararg, argThat, eq => mockitoEq}
import org.mockito.Mockito.{times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.SparkPodInitContainerBootstrap
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.constants._

class ClientV2Suite extends SparkFunSuite with BeforeAndAfter {

  private val JARS_RESOURCE = SubmittedResourceIdAndSecret("jarsId", "jarsSecret")
  private val FILES_RESOURCE = SubmittedResourceIdAndSecret("filesId", "filesSecret")
  private val SUBMITTED_RESOURCES = SubmittedResources(JARS_RESOURCE, FILES_RESOURCE)
  private val BOOTSTRAPPED_POD_ANNOTATION = "bootstrapped"
  private val TRUE = "true"
  private val APP_NAME = "spark-test"
  private val APP_ID = "spark-app-id"
  private val CUSTOM_LABEL_KEY = "customLabel"
  private val CUSTOM_LABEL_VALUE = "customLabelValue"
  private val ALL_EXPECTED_LABELS = Map(
      CUSTOM_LABEL_KEY -> CUSTOM_LABEL_VALUE,
      SPARK_APP_ID_LABEL -> APP_ID,
      SPARK_APP_NAME_LABEL -> APP_NAME)
  private val CUSTOM_ANNOTATION_KEY = "customAnnotation"
  private val CUSTOM_ANNOTATION_VALUE = "customAnnotationValue"
  private val SECRET_NAME = "secret"
  private val SECRET_KEY = "secret-key"
  private val SECRET_DATA = "secret-data"
  private val MAIN_CLASS = "org.apache.spark.examples.SparkPi"
  private val APP_ARGS = Array("3", "20")
  private val SPARK_JARS = Seq(
      "hdfs://localhost:9000/app/jars/jar1.jar", "file:///app/jars/jar2.jar")
  private val RESOLVED_SPARK_JARS = Seq(
    "hdfs://localhost:9000/app/jars/jar1.jar", "file:///var/data/spark-jars/jar2.jar")
  private val RESOLVED_SPARK_REMOTE_AND_LOCAL_JARS = Seq(
    "/var/data/spark-jars/jar1.jar", "/var/data/spark-jars/jar2.jar")
  private val SPARK_FILES = Seq(
    "hdfs://localhost:9000/app/files/file1.txt", "file:///app/files/file2.txt")
  private val RESOLVED_SPARK_FILES = Seq(
    "hdfs://localhost:9000/app/files/file1.txt", "file:///var/data/spark-files/file2.txt")
  private val INIT_CONTAINER_SECRET = new SecretBuilder()
    .withNewMetadata()
      .withName(SECRET_NAME)
      .endMetadata()
    .addToData(SECRET_KEY, SECRET_DATA)
    .build()
  private val CONFIG_MAP_NAME = "config-map"
  private val CONFIG_MAP_KEY = "config-map-key"
  private val CONFIG_MAP_DATA = "config-map-data"
  private val CUSTOM_JAVA_OPTION_KEY = "myappoption"
  private val CUSTOM_JAVA_OPTION_VALUE = "myappoptionvalue"
  private val DRIVER_JAVA_OPTIONS = s"-D$CUSTOM_JAVA_OPTION_KEY=$CUSTOM_JAVA_OPTION_VALUE"
  private val DRIVER_EXTRA_CLASSPATH = "/var/data/spark-app-custom/custom-jar.jar"
  private val INIT_CONTAINER_CONFIG_MAP = new ConfigMapBuilder()
    .withNewMetadata()
      .withName(CONFIG_MAP_NAME)
      .endMetadata()
    .addToData(CONFIG_MAP_KEY, CONFIG_MAP_DATA)
    .build()
  private val CUSTOM_DRIVER_IMAGE = "spark-custom-driver:latest"
  private val DRIVER_MEMORY_MB = 512
  private val DRIVER_MEMORY_OVERHEAD_MB = 128
  private val SPARK_CONF = new SparkConf(true)
      .set(DRIVER_DOCKER_IMAGE, CUSTOM_DRIVER_IMAGE)
      .set(org.apache.spark.internal.config.DRIVER_MEMORY, DRIVER_MEMORY_MB.toLong)
      .set(KUBERNETES_DRIVER_MEMORY_OVERHEAD, DRIVER_MEMORY_OVERHEAD_MB.toLong)
      .set(KUBERNETES_DRIVER_LABELS, s"$CUSTOM_LABEL_KEY=$CUSTOM_LABEL_VALUE")
      .set(KUBERNETES_DRIVER_ANNOTATIONS, s"$CUSTOM_ANNOTATION_KEY=$CUSTOM_ANNOTATION_VALUE")
      .set(org.apache.spark.internal.config.DRIVER_CLASS_PATH, DRIVER_EXTRA_CLASSPATH)
      .set(org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS, DRIVER_JAVA_OPTIONS)
  private val EXECUTOR_INIT_CONF_KEY = "executor-init-conf"
  private val SPARK_CONF_WITH_EXECUTOR_INIT_CONF = SPARK_CONF.clone()
      .set(EXECUTOR_INIT_CONF_KEY, TRUE)
  private val DRIVER_POD_UID = "driver-pod-uid"
  private val DRIVER_POD_KIND = "pod"
  private val DRIVER_POD_API_VERSION = "v1"
  @Mock
  private var initContainerConfigMapBuilder: SparkInitContainerConfigMapBuilder = _
  @Mock
  private var containerLocalizedFilesResolver: ContainerLocalizedFilesResolver = _
  @Mock
  private var executorInitContainerConfiguration: ExecutorInitContainerConfiguration = _
  @Mock
  private var submittedDependencyUploader: SubmittedDependencyUploader = _
  @Mock
  private var submittedDependenciesSecretBuilder: SubmittedDependencySecretBuilder = _
  @Mock
  private var initContainerBootstrap: SparkPodInitContainerBootstrap = _
  @Mock
  private var initContainerComponentsProvider: DriverInitContainerComponentsProvider = _
  @Mock
  private var kubernetesClientProvider: SubmissionKubernetesClientProvider = _
  @Mock
  private var kubernetesClient: KubernetesClient = _
  @Mock
  private var podOps: MixedOperation[Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]] = _
  private type ResourceListOps = NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable[
      HasMetadata, java.lang.Boolean]
  @Mock
  private var resourceListOps: ResourceListOps = _

  before {
    MockitoAnnotations.initMocks(this)
    when(initContainerComponentsProvider.provideInitContainerBootstrap())
      .thenReturn(initContainerBootstrap)
    when(submittedDependencyUploader.uploadJars()).thenReturn(JARS_RESOURCE)
    when(submittedDependencyUploader.uploadFiles()).thenReturn(FILES_RESOURCE)
    when(initContainerBootstrap
      .bootstrapInitContainerAndVolumes(mockitoEq(DRIVER_CONTAINER_NAME), any()))
      .thenAnswer(new Answer[PodBuilder] {
        override def answer(invocationOnMock: InvocationOnMock): PodBuilder = {
          invocationOnMock.getArgumentAt(1, classOf[PodBuilder]).editMetadata()
            .addToAnnotations(BOOTSTRAPPED_POD_ANNOTATION, TRUE)
            .endMetadata()
        }
      })
    when(initContainerComponentsProvider.provideContainerLocalizedFilesResolver())
      .thenReturn(containerLocalizedFilesResolver)
    when(initContainerComponentsProvider.provideExecutorInitContainerConfiguration())
      .thenReturn(executorInitContainerConfiguration)
    when(submittedDependenciesSecretBuilder.build())
      .thenReturn(INIT_CONTAINER_SECRET)
    when(initContainerConfigMapBuilder.build())
      .thenReturn(INIT_CONTAINER_CONFIG_MAP)
    when(kubernetesClientProvider.get).thenReturn(kubernetesClient)
    when(kubernetesClient.pods()).thenReturn(podOps)
    when(podOps.create(any())).thenAnswer(new Answer[Pod] {
      override def answer(invocation: InvocationOnMock): Pod = {
        new PodBuilder(invocation.getArgumentAt(0, classOf[Pod]))
          .editMetadata()
          .withUid(DRIVER_POD_UID)
          .endMetadata()
          .withKind(DRIVER_POD_KIND)
          .withApiVersion(DRIVER_POD_API_VERSION)
          .build()
      }
    })
    when(containerLocalizedFilesResolver.resolveSubmittedAndRemoteSparkJars())
        .thenReturn(RESOLVED_SPARK_REMOTE_AND_LOCAL_JARS)
    when(containerLocalizedFilesResolver.resolveSubmittedSparkJars())
        .thenReturn(RESOLVED_SPARK_JARS)
    when(containerLocalizedFilesResolver.resolveSubmittedSparkFiles())
        .thenReturn(RESOLVED_SPARK_FILES)
    when(executorInitContainerConfiguration.configureSparkConfForExecutorInitContainer(SPARK_CONF))
        .thenReturn(SPARK_CONF_WITH_EXECUTOR_INIT_CONF)
    when(kubernetesClient.resourceList(anyVararg[HasMetadata]())).thenReturn(resourceListOps)
  }

  test("Run with dependency uploader") {
    when(initContainerComponentsProvider
        .provideInitContainerSubmittedDependencyUploader(ALL_EXPECTED_LABELS))
        .thenReturn(Some(submittedDependencyUploader))
    when(initContainerComponentsProvider
        .provideSubmittedDependenciesSecretBuilder(Some(SUBMITTED_RESOURCES.secrets())))
        .thenReturn(Some(submittedDependenciesSecretBuilder))
    when(initContainerComponentsProvider
        .provideInitContainerConfigMapBuilder(Some(SUBMITTED_RESOURCES.ids())))
        .thenReturn(initContainerConfigMapBuilder)
    runAndVerifyDriverPodHasCorrectProperties()
    val resourceListArgumentCaptor = ArgumentCaptor.forClass(classOf[HasMetadata])
    verify(kubernetesClient).resourceList(resourceListArgumentCaptor.capture())
    val createdResources = resourceListArgumentCaptor.getAllValues.asScala
    assert(createdResources.size === 2)
    verifyCreatedResourcesHaveOwnerReferences(createdResources)
    assert(createdResources.exists {
      case secret: Secret =>
        val expectedSecretData = Map(SECRET_KEY -> SECRET_DATA)
        secret.getMetadata.getName == SECRET_NAME && secret.getData.asScala == expectedSecretData
      case _ => false
    })
    verifyConfigMapWasCreated(createdResources)
    verify(submittedDependencyUploader).uploadJars()
    verify(submittedDependencyUploader).uploadFiles()
    verify(initContainerComponentsProvider)
        .provideInitContainerConfigMapBuilder(Some(SUBMITTED_RESOURCES.ids()))
    verify(initContainerComponentsProvider)
      .provideSubmittedDependenciesSecretBuilder(Some(SUBMITTED_RESOURCES.secrets()))
  }

  test("Run without dependency uploader") {
    when(initContainerComponentsProvider
      .provideInitContainerSubmittedDependencyUploader(ALL_EXPECTED_LABELS))
      .thenReturn(None)
    when(initContainerComponentsProvider
      .provideSubmittedDependenciesSecretBuilder(None))
      .thenReturn(None)
    when(initContainerComponentsProvider
      .provideInitContainerConfigMapBuilder(None))
      .thenReturn(initContainerConfigMapBuilder)
    runAndVerifyDriverPodHasCorrectProperties()
    val resourceListArgumentCaptor = ArgumentCaptor.forClass(classOf[HasMetadata])
    verify(kubernetesClient).resourceList(resourceListArgumentCaptor.capture())
    val createdResources = resourceListArgumentCaptor.getAllValues.asScala
    assert(createdResources.size === 1)
    verifyCreatedResourcesHaveOwnerReferences(createdResources)
    verifyConfigMapWasCreated(createdResources)
    verify(submittedDependencyUploader, times(0)).uploadJars()
    verify(submittedDependencyUploader, times(0)).uploadFiles()
    verify(initContainerComponentsProvider)
      .provideInitContainerConfigMapBuilder(None)
    verify(initContainerComponentsProvider)
      .provideSubmittedDependenciesSecretBuilder(None)
  }

  private def verifyCreatedResourcesHaveOwnerReferences(
      createdResources: mutable.Buffer[HasMetadata]): Unit = {
    assert(createdResources.forall { resource =>
      val owners = resource.getMetadata.getOwnerReferences.asScala
      owners.size === 1 &&
        owners.head.getController &&
        owners.head.getKind == DRIVER_POD_KIND &&
        owners.head.getUid == DRIVER_POD_UID &&
        owners.head.getName == APP_ID &&
        owners.head.getApiVersion == DRIVER_POD_API_VERSION
    })
  }

  private def verifyConfigMapWasCreated(createdResources: mutable.Buffer[HasMetadata]): Unit = {
    assert(createdResources.exists {
      case configMap: ConfigMap =>
        val expectedConfigMapData = Map(CONFIG_MAP_KEY -> CONFIG_MAP_DATA)
        configMap.getMetadata.getName == CONFIG_MAP_NAME &&
          configMap.getData.asScala == expectedConfigMapData
      case _ => false
    })
  }

  private def runAndVerifyDriverPodHasCorrectProperties(): Unit = {
    new Client(
      APP_NAME,
      APP_ID,
      MAIN_CLASS,
      SPARK_CONF,
      APP_ARGS,
      SPARK_JARS,
      SPARK_FILES,
      kubernetesClientProvider,
      initContainerComponentsProvider).run()
    val podMatcher = new BaseMatcher[Pod] {
      override def matches(o: scala.Any): Boolean = {
        o match {
          case p: Pod =>
            Option(p)
              .filter(_.getMetadata.getName == APP_ID)
              .filter(podHasCorrectAnnotations)
              .filter(_.getMetadata.getLabels.asScala == ALL_EXPECTED_LABELS)
              .filter(containerHasCorrectBasicContainerConfiguration)
              .filter(containerHasCorrectBasicEnvs)
              .filter(containerHasCorrectMountedClasspath)
              .exists(containerHasCorrectJvmOptions)
          case _ =>
            false
        }
      }

      override def describeTo(description: Description): Unit = {}
    }
    verify(podOps).create(argThat(podMatcher))
  }

  private def containerHasCorrectJvmOptions(pod: Pod): Boolean = {
    val driverContainer = pod.getSpec.getContainers.asScala.head
    val envs = driverContainer.getEnv.asScala.map(env => (env.getName, env.getValue))
    envs.toMap.get(ENV_DRIVER_JAVA_OPTS).exists { javaOptions =>
      val splitOptions = javaOptions.split(" ")
      val expectedOptions = SPARK_CONF.getAll
        .filterNot(_._1 == org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS.key)
        .toMap ++
        Map(
          "spark.app.id" -> APP_ID,
          KUBERNETES_DRIVER_POD_NAME.key -> APP_ID,
          EXECUTOR_INIT_CONF_KEY -> TRUE,
          CUSTOM_JAVA_OPTION_KEY -> CUSTOM_JAVA_OPTION_VALUE,
          "spark.jars" -> RESOLVED_SPARK_JARS.mkString(","),
          "spark.files" -> RESOLVED_SPARK_FILES.mkString(","))
      splitOptions.forall(_.startsWith("-D")) &&
        splitOptions.map { option =>
          val withoutPrefix = option.substring(2)
          (withoutPrefix.split("=", 2)(0), withoutPrefix.split("=", 2)(1))
        }.toMap == expectedOptions
    }
  }

  private def containerHasCorrectMountedClasspath(pod: Pod): Boolean = {
    val driverContainer = pod.getSpec.getContainers.asScala.head
    val envs = driverContainer.getEnv.asScala.map(env => (env.getName, env.getValue))
    envs.toMap.get(ENV_MOUNTED_CLASSPATH).exists { classpath =>
      val mountedClasspathEntities = classpath.split(File.pathSeparator)
      mountedClasspathEntities.toSet == RESOLVED_SPARK_REMOTE_AND_LOCAL_JARS.toSet
    }
  }

  private def containerHasCorrectBasicEnvs(pod: Pod): Boolean = {
    val driverContainer = pod.getSpec.getContainers.asScala.head
    val envs = driverContainer.getEnv.asScala.map(env => (env.getName, env.getValue))
    val expectedBasicEnvs = Map(
      ENV_SUBMIT_EXTRA_CLASSPATH -> DRIVER_EXTRA_CLASSPATH,
      ENV_DRIVER_MEMORY -> s"${DRIVER_MEMORY_MB + DRIVER_MEMORY_OVERHEAD_MB}m",
      ENV_DRIVER_MAIN_CLASS -> MAIN_CLASS,
      ENV_DRIVER_ARGS -> APP_ARGS.mkString(" "))
    expectedBasicEnvs.toSet.subsetOf(envs.toSet)
  }

  private def containerHasCorrectBasicContainerConfiguration(pod: Pod): Boolean = {
    val containers = pod.getSpec.getContainers.asScala
    containers.size == 1 &&
      containers.head.getName == DRIVER_CONTAINER_NAME &&
      containers.head.getImage == CUSTOM_DRIVER_IMAGE &&
      containers.head.getImagePullPolicy == "IfNotPresent"
  }

  private def podHasCorrectAnnotations(pod: Pod): Boolean = {
    val expectedAnnotations = Map(
      CUSTOM_ANNOTATION_KEY -> CUSTOM_ANNOTATION_VALUE,
      BOOTSTRAPPED_POD_ANNOTATION -> TRUE)
    pod.getMetadata.getAnnotations.asScala == expectedAnnotations
  }
}
