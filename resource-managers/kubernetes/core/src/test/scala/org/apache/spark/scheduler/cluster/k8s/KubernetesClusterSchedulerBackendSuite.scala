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
package org.apache.spark.scheduler.cluster.k8s

import java.util.concurrent.{ExecutorService, ScheduledExecutorService, TimeUnit}

import io.fabric8.kubernetes.api.model.{DoneablePod, Pod, PodBuilder, PodList}
import io.fabric8.kubernetes.client.{KubernetesClient, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.dsl.{FilterWatchListDeletable, MixedOperation, NonNamespaceOperation, PodResource}
import org.mockito.{AdditionalAnswers, ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Matchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{doNothing, never, times, verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar._
import scala.collection.JavaConverters._
import scala.concurrent.Future

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointAddress, RpcEndpointRef, RpcEnv, RpcTimeout}
import org.apache.spark.scheduler.{ExecutorExited, LiveListenerBus, SlaveLost, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterExecutor, RemoveExecutor}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

private[spark] class KubernetesClusterSchedulerBackendSuite
    extends SparkFunSuite with BeforeAndAfter {

  private val APP_ID = "test-spark-app"
  private val DRIVER_POD_NAME = "spark-driver-pod"
  private val NAMESPACE = "test-namespace"
  private val SPARK_DRIVER_HOST = "localhost"
  private val SPARK_DRIVER_PORT = 7077
  private val POD_ALLOCATION_INTERVAL = 60L
  private val DRIVER_URL = RpcEndpointAddress(
      SPARK_DRIVER_HOST, SPARK_DRIVER_PORT, CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
  private val FIRST_EXECUTOR_POD = new PodBuilder()
    .withNewMetadata()
      .withName("pod1")
      .endMetadata()
    .withNewSpec()
      .withNodeName("node1")
      .endSpec()
    .withNewStatus()
      .withHostIP("192.168.99.100")
      .endStatus()
    .build()
  private val SECOND_EXECUTOR_POD = new PodBuilder()
    .withNewMetadata()
      .withName("pod2")
      .endMetadata()
    .withNewSpec()
      .withNodeName("node2")
      .endSpec()
    .withNewStatus()
      .withHostIP("192.168.99.101")
      .endStatus()
    .build()

  private type PODS = MixedOperation[Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]]
  private type LABELLED_PODS = FilterWatchListDeletable[
      Pod, PodList, java.lang.Boolean, Watch, Watcher[Pod]]
  private type IN_NAMESPACE_PODS = NonNamespaceOperation[
      Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]]

  @Mock
  private var sparkContext: SparkContext = _

  @Mock
  private var listenerBus: LiveListenerBus = _

  @Mock
  private var taskSchedulerImpl: TaskSchedulerImpl = _

  @Mock
  private var allocatorExecutor: ScheduledExecutorService = _

  @Mock
  private var requestExecutorsService: ExecutorService = _

  @Mock
  private var executorPodFactory: ExecutorPodFactory = _

  @Mock
  private var shuffleManager: KubernetesExternalShuffleManager = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var podsWithLabelOperations: LABELLED_PODS = _

  @Mock
  private var podsInNamespace: IN_NAMESPACE_PODS = _

  @Mock
  private var podsWithDriverName: PodResource[Pod, DoneablePod] = _

  @Mock
  private var rpcEnv: RpcEnv = _

  @Mock
  private var driverEndpointRef: RpcEndpointRef = _

  @Mock
  private var executorPodsWatch: Watch = _

  private var sparkConf: SparkConf = _
  private var executorPodsWatcherArgument: ArgumentCaptor[Watcher[Pod]] = _
  private var allocatorRunnable: ArgumentCaptor[Runnable] = _
  private var requestExecutorRunnable: ArgumentCaptor[Runnable] = _
  private var driverEndpoint: ArgumentCaptor[RpcEndpoint] = _

  private val driverPod = new PodBuilder()
    .withNewMetadata()
      .withName(DRIVER_POD_NAME)
      .addToLabels(SPARK_APP_ID_LABEL, APP_ID)
      .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_DRIVER_ROLE)
      .endMetadata()
    .build()

  before {
    MockitoAnnotations.initMocks(this)
    sparkConf = new SparkConf()
        .set("spark.app.id", APP_ID)
        .set(KUBERNETES_DRIVER_POD_NAME, DRIVER_POD_NAME)
        .set(KUBERNETES_NAMESPACE, NAMESPACE)
        .set("spark.driver.host", SPARK_DRIVER_HOST)
        .set("spark.driver.port", SPARK_DRIVER_PORT.toString)
        .set(KUBERNETES_ALLOCATION_BATCH_DELAY, POD_ALLOCATION_INTERVAL)
    executorPodsWatcherArgument = ArgumentCaptor.forClass(classOf[Watcher[Pod]])
    allocatorRunnable = ArgumentCaptor.forClass(classOf[Runnable])
    requestExecutorRunnable = ArgumentCaptor.forClass(classOf[Runnable])
    driverEndpoint = ArgumentCaptor.forClass(classOf[RpcEndpoint])
    when(sparkContext.conf).thenReturn(sparkConf)
    when(sparkContext.listenerBus).thenReturn(listenerBus)
    when(taskSchedulerImpl.sc).thenReturn(sparkContext)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withLabel(SPARK_APP_ID_LABEL, APP_ID)).thenReturn(podsWithLabelOperations)
    when(podsWithLabelOperations.watch(executorPodsWatcherArgument.capture()))
        .thenReturn(executorPodsWatch)
    when(podOperations.inNamespace(NAMESPACE)).thenReturn(podsInNamespace)
    when(podsInNamespace.withName(DRIVER_POD_NAME)).thenReturn(podsWithDriverName)
    when(podsWithDriverName.get()).thenReturn(driverPod)
    when(allocatorExecutor.scheduleWithFixedDelay(
        allocatorRunnable.capture(),
        mockitoEq(0L),
        mockitoEq(POD_ALLOCATION_INTERVAL),
        mockitoEq(TimeUnit.SECONDS))).thenReturn(null)
    // Creating Futures in Scala backed by a Java executor service resolves to running
    // ExecutorService#execute (as opposed to submit)
    doNothing().when(requestExecutorsService).execute(requestExecutorRunnable.capture())
    when(rpcEnv.setupEndpoint(
        mockitoEq(CoarseGrainedSchedulerBackend.ENDPOINT_NAME), driverEndpoint.capture()))
        .thenReturn(driverEndpointRef)
    when(driverEndpointRef.ask[Boolean]
      (any(classOf[Any]))
      (any())).thenReturn(mock[Future[Boolean]])
  }

  test("Basic lifecycle expectations when starting and stopping the scheduler.") {
    val scheduler = newSchedulerBackend(true)
    scheduler.start()
    verify(shuffleManager).start(APP_ID)
    assert(executorPodsWatcherArgument.getValue != null)
    assert(allocatorRunnable.getValue != null)
    scheduler.stop()
    verify(shuffleManager).stop()
    verify(executorPodsWatch).close()
  }

  test("Static allocation should request executors upon first allocator run.") {
    sparkConf
        .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 2)
        .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 2)
    val scheduler = newSchedulerBackend(true)
    scheduler.start()
    requestExecutorRunnable.getValue.run()
    expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
    expectPodCreationWithId(2, SECOND_EXECUTOR_POD)
    when(podOperations.create(any(classOf[Pod]))).thenAnswer(AdditionalAnswers.returnsFirstArg())
    allocatorRunnable.getValue.run()
    verify(podOperations).create(FIRST_EXECUTOR_POD)
    verify(podOperations).create(SECOND_EXECUTOR_POD)
  }

  test("Killing executors deletes the executor pods") {
    sparkConf
        .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 2)
        .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 2)
    val scheduler = newSchedulerBackend(true)
    scheduler.start()
    requestExecutorRunnable.getValue.run()
    expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
    expectPodCreationWithId(2, SECOND_EXECUTOR_POD)
    when(podOperations.create(any(classOf[Pod])))
        .thenAnswer(AdditionalAnswers.returnsFirstArg())
    allocatorRunnable.getValue.run()
    scheduler.doKillExecutors(Seq("2"))
    requestExecutorRunnable.getAllValues.asScala.last.run()
    verify(podOperations).delete(SECOND_EXECUTOR_POD)
    verify(podOperations, never()).delete(FIRST_EXECUTOR_POD)
  }

  test("Executors should be requested in batches.") {
    sparkConf
        .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 1)
        .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 2)
    val scheduler = newSchedulerBackend(true)
    scheduler.start()
    requestExecutorRunnable.getValue.run()
    when(podOperations.create(any(classOf[Pod])))
      .thenAnswer(AdditionalAnswers.returnsFirstArg())
    expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
    expectPodCreationWithId(2, SECOND_EXECUTOR_POD)
    allocatorRunnable.getValue.run()
    verify(podOperations).create(FIRST_EXECUTOR_POD)
    verify(podOperations, never()).create(SECOND_EXECUTOR_POD)
    val registerFirstExecutorMessage = RegisterExecutor(
        "1", mock[RpcEndpointRef], "localhost", 1, Map.empty[String, String])
    when(taskSchedulerImpl.resourceOffers(any())).thenReturn(Seq.empty)
    driverEndpoint.getValue.receiveAndReply(mock[RpcCallContext])
        .apply(registerFirstExecutorMessage)
    allocatorRunnable.getValue.run()
    verify(podOperations).create(SECOND_EXECUTOR_POD)
  }

  test("Deleting executors and then running an allocator pass after finding the loss reason" +
      " should only delete the pod once.") {
    sparkConf
        .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 1)
        .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 1)
    val scheduler = newSchedulerBackend(true)
    scheduler.start()
    requestExecutorRunnable.getValue.run()
    when(podOperations.create(any(classOf[Pod])))
        .thenAnswer(AdditionalAnswers.returnsFirstArg())
    expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
    allocatorRunnable.getValue.run()
    val executorEndpointRef = mock[RpcEndpointRef]
    when(executorEndpointRef.address).thenReturn(RpcAddress("pod.example.com", 9000))
    val registerFirstExecutorMessage = RegisterExecutor(
        "1", executorEndpointRef, "localhost:9000", 1, Map.empty[String, String])
    when(taskSchedulerImpl.resourceOffers(any())).thenReturn(Seq.empty)
    driverEndpoint.getValue.receiveAndReply(mock[RpcCallContext])
        .apply(registerFirstExecutorMessage)
    scheduler.doRequestTotalExecutors(0)
    requestExecutorRunnable.getAllValues.asScala.last.run()
    scheduler.doKillExecutors(Seq("1"))
    requestExecutorRunnable.getAllValues.asScala.last.run()
    verify(podOperations, times(1)).delete(FIRST_EXECUTOR_POD)
    driverEndpoint.getValue.onDisconnected(executorEndpointRef.address)

    val exitedPod = exitPod(FIRST_EXECUTOR_POD, 0)
    executorPodsWatcherArgument.getValue.eventReceived(Action.DELETED, exitedPod)
    allocatorRunnable.getValue.run()
    verify(podOperations, times(1)).delete(FIRST_EXECUTOR_POD)
    verify(driverEndpointRef, times(1)).ask[Boolean](
        RemoveExecutor("1", ExecutorExited(
            0,
            exitCausedByApp = false,
            s"Container in pod ${exitedPod.getMetadata.getName} exited from" +
              s" explicit termination request.")))
  }

  test("Executors that disconnect from application errors are noted as exits caused by app.") {
    sparkConf
        .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 1)
        .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 1)
    val scheduler = newSchedulerBackend(true)
    scheduler.start()
    expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
    when(podOperations.create(any(classOf[Pod]))).thenAnswer(AdditionalAnswers.returnsFirstArg())
    requestExecutorRunnable.getValue.run()
    allocatorRunnable.getValue.run()
    val executorEndpointRef = mock[RpcEndpointRef]
    when(executorEndpointRef.address).thenReturn(RpcAddress("pod.example.com", 9000))
    val registerFirstExecutorMessage = RegisterExecutor(
        "1", executorEndpointRef, "localhost:9000", 1, Map.empty[String, String])
    when(taskSchedulerImpl.resourceOffers(any())).thenReturn(Seq.empty)
    driverEndpoint.getValue.receiveAndReply(mock[RpcCallContext])
        .apply(registerFirstExecutorMessage)
    driverEndpoint.getValue.onDisconnected(executorEndpointRef.address)
    executorPodsWatcherArgument.getValue.eventReceived(
        Action.ERROR, exitPod(FIRST_EXECUTOR_POD, 1))

    expectPodCreationWithId(2, SECOND_EXECUTOR_POD)
    scheduler.doRequestTotalExecutors(1)
    requestExecutorRunnable.getValue.run()
    allocatorRunnable.getAllValues.asScala.last.run()
    verify(driverEndpointRef).ask[Boolean](
        RemoveExecutor("1", ExecutorExited(
            1,
            exitCausedByApp = true,
            s"Pod ${FIRST_EXECUTOR_POD.getMetadata.getName}'s executor container exited with" +
              " exit status code 1.")))
    verify(podOperations, never()).delete(FIRST_EXECUTOR_POD)
  }

  test("Executors should only try to get the loss reason a number of times before giving up and" +
    " removing the executor.") {
    sparkConf
        .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 1)
        .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 1)
    val scheduler = newSchedulerBackend(true)
    scheduler.start()
    expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
    when(podOperations.create(any(classOf[Pod]))).thenAnswer(AdditionalAnswers.returnsFirstArg())
    requestExecutorRunnable.getValue.run()
    allocatorRunnable.getValue.run()
    val executorEndpointRef = mock[RpcEndpointRef]
    when(executorEndpointRef.address).thenReturn(RpcAddress("pod.example.com", 9000))
    val registerFirstExecutorMessage = RegisterExecutor(
        "1", executorEndpointRef, "localhost:9000", 1, Map.empty[String, String])
    when(taskSchedulerImpl.resourceOffers(any())).thenReturn(Seq.empty)
    driverEndpoint.getValue.receiveAndReply(mock[RpcCallContext])
        .apply(registerFirstExecutorMessage)
    driverEndpoint.getValue.onDisconnected(executorEndpointRef.address)
    1 to KubernetesClusterSchedulerBackend.MAX_EXECUTOR_LOST_REASON_CHECKS foreach { _ =>
      allocatorRunnable.getValue.run()
      verify(podOperations, never()).delete(FIRST_EXECUTOR_POD)
    }
    expectPodCreationWithId(2, SECOND_EXECUTOR_POD)
    allocatorRunnable.getValue.run()
    verify(podOperations).delete(FIRST_EXECUTOR_POD)
    verify(driverEndpointRef).ask[Boolean](
      RemoveExecutor("1", SlaveLost("Executor lost for unknown reasons.")))
  }

  private def newSchedulerBackend(externalShuffle: Boolean): KubernetesClusterSchedulerBackend = {
    new KubernetesClusterSchedulerBackend(
        taskSchedulerImpl,
        rpcEnv,
        executorPodFactory,
        if (externalShuffle) Some(shuffleManager) else None,
        kubernetesClient,
        allocatorExecutor,
        requestExecutorsService)
  }

  private def exitPod(basePod: Pod, exitCode: Int): Pod = {
    new PodBuilder(FIRST_EXECUTOR_POD)
      .editStatus()
        .addNewContainerStatus()
          .withNewState()
            .withNewTerminated()
              .withExitCode(exitCode)
              .endTerminated()
            .endState()
          .endContainerStatus()
        .endStatus()
      .build()
  }

  private def expectPodCreationWithId(executorId: Int, expectedPod: Pod): Unit = {
    when(executorPodFactory.createExecutorPod(
        executorId.toString,
        APP_ID,
        DRIVER_URL,
        sparkConf.getExecutorEnv,
        driverPod,
        Map.empty)).thenReturn(expectedPod)
  }

}
