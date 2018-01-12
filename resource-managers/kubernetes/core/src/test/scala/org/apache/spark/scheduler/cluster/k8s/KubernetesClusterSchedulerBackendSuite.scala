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
<<<<<<< HEAD
import org.scalatest.mock.MockitoSugar._
=======
import org.scalatest.mockito.MockitoSugar._
>>>>>>> master
import scala.collection.JavaConverters._
import scala.concurrent.Future

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
<<<<<<< HEAD
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointAddress, RpcEndpointRef, RpcEnv, RpcTimeout}
import org.apache.spark.scheduler.{ExecutorExited, LiveListenerBus, SlaveLost, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterExecutor, RemoveExecutor}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

private[spark] class KubernetesClusterSchedulerBackendSuite
    extends SparkFunSuite with BeforeAndAfter {
=======
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.rpc._
import org.apache.spark.scheduler.{ExecutorExited, LiveListenerBus, SlaveLost, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterExecutor, RemoveExecutor}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.ThreadUtils

class KubernetesClusterSchedulerBackendSuite extends SparkFunSuite with BeforeAndAfter {
>>>>>>> master

  private val APP_ID = "test-spark-app"
  private val DRIVER_POD_NAME = "spark-driver-pod"
  private val NAMESPACE = "test-namespace"
  private val SPARK_DRIVER_HOST = "localhost"
  private val SPARK_DRIVER_PORT = 7077
<<<<<<< HEAD
  private val POD_ALLOCATION_INTERVAL = 60L
  private val DRIVER_URL = RpcEndpointAddress(
      SPARK_DRIVER_HOST, SPARK_DRIVER_PORT, CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
=======
  private val POD_ALLOCATION_INTERVAL = "1m"
  private val DRIVER_URL = RpcEndpointAddress(
    SPARK_DRIVER_HOST, SPARK_DRIVER_PORT, CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
>>>>>>> master
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
<<<<<<< HEAD
  private type LABELLED_PODS = FilterWatchListDeletable[
      Pod, PodList, java.lang.Boolean, Watch, Watcher[Pod]]
  private type IN_NAMESPACE_PODS = NonNamespaceOperation[
      Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]]
=======
  private type LABELED_PODS = FilterWatchListDeletable[
    Pod, PodList, java.lang.Boolean, Watch, Watcher[Pod]]
  private type IN_NAMESPACE_PODS = NonNamespaceOperation[
    Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]]
>>>>>>> master

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
<<<<<<< HEAD
  private var shuffleManager: KubernetesExternalShuffleManager = _

  @Mock
=======
>>>>>>> master
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
<<<<<<< HEAD
  private var podsWithLabelOperations: LABELLED_PODS = _
=======
  private var podsWithLabelOperations: LABELED_PODS = _
>>>>>>> master

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

<<<<<<< HEAD
=======
  @Mock
  private var successFuture: Future[Boolean] = _

>>>>>>> master
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
<<<<<<< HEAD
        .set("spark.app.id", APP_ID)
        .set(KUBERNETES_DRIVER_POD_NAME, DRIVER_POD_NAME)
        .set(KUBERNETES_NAMESPACE, NAMESPACE)
        .set("spark.driver.host", SPARK_DRIVER_HOST)
        .set("spark.driver.port", SPARK_DRIVER_PORT.toString)
        .set(KUBERNETES_ALLOCATION_BATCH_DELAY, POD_ALLOCATION_INTERVAL)
=======
      .set(KUBERNETES_DRIVER_POD_NAME, DRIVER_POD_NAME)
      .set(KUBERNETES_NAMESPACE, NAMESPACE)
      .set("spark.driver.host", SPARK_DRIVER_HOST)
      .set("spark.driver.port", SPARK_DRIVER_PORT.toString)
      .set(KUBERNETES_ALLOCATION_BATCH_DELAY.key, POD_ALLOCATION_INTERVAL)
>>>>>>> master
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
<<<<<<< HEAD
        .thenReturn(executorPodsWatch)
=======
      .thenReturn(executorPodsWatch)
>>>>>>> master
    when(podOperations.inNamespace(NAMESPACE)).thenReturn(podsInNamespace)
    when(podsInNamespace.withName(DRIVER_POD_NAME)).thenReturn(podsWithDriverName)
    when(podsWithDriverName.get()).thenReturn(driverPod)
    when(allocatorExecutor.scheduleWithFixedDelay(
<<<<<<< HEAD
        allocatorRunnable.capture(),
        mockitoEq(0L),
        mockitoEq(POD_ALLOCATION_INTERVAL),
        mockitoEq(TimeUnit.SECONDS))).thenReturn(null)
=======
      allocatorRunnable.capture(),
      mockitoEq(0L),
      mockitoEq(TimeUnit.MINUTES.toMillis(1)),
      mockitoEq(TimeUnit.MILLISECONDS))).thenReturn(null)
>>>>>>> master
    // Creating Futures in Scala backed by a Java executor service resolves to running
    // ExecutorService#execute (as opposed to submit)
    doNothing().when(requestExecutorsService).execute(requestExecutorRunnable.capture())
    when(rpcEnv.setupEndpoint(
<<<<<<< HEAD
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
=======
      mockitoEq(CoarseGrainedSchedulerBackend.ENDPOINT_NAME), driverEndpoint.capture()))
      .thenReturn(driverEndpointRef)

    // Used by the CoarseGrainedSchedulerBackend when making RPC calls.
    when(driverEndpointRef.ask[Boolean]
      (any(classOf[Any]))
      (any())).thenReturn(successFuture)
    when(successFuture.failed).thenReturn(Future[Throwable] {
      // emulate behavior of the Future.failed method.
      throw new NoSuchElementException()
    }(ThreadUtils.sameThread))
  }

  test("Basic lifecycle expectations when starting and stopping the scheduler.") {
    val scheduler = newSchedulerBackend()
    scheduler.start()
    assert(executorPodsWatcherArgument.getValue != null)
    assert(allocatorRunnable.getValue != null)
    scheduler.stop()
>>>>>>> master
    verify(executorPodsWatch).close()
  }

  test("Static allocation should request executors upon first allocator run.") {
    sparkConf
<<<<<<< HEAD
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
=======
      .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 2)
      .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 2)
    val scheduler = newSchedulerBackend()
    scheduler.start()
    requestExecutorRunnable.getValue.run()
    val firstResolvedPod = expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
    val secondResolvedPod = expectPodCreationWithId(2, SECOND_EXECUTOR_POD)
    when(podOperations.create(any(classOf[Pod]))).thenAnswer(AdditionalAnswers.returnsFirstArg())
    allocatorRunnable.getValue.run()
    verify(podOperations).create(firstResolvedPod)
    verify(podOperations).create(secondResolvedPod)
>>>>>>> master
  }

  test("Killing executors deletes the executor pods") {
    sparkConf
<<<<<<< HEAD
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
=======
      .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 2)
      .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 2)
    val scheduler = newSchedulerBackend()
    scheduler.start()
    requestExecutorRunnable.getValue.run()
    val firstResolvedPod = expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
    val secondResolvedPod = expectPodCreationWithId(2, SECOND_EXECUTOR_POD)
    when(podOperations.create(any(classOf[Pod])))
      .thenAnswer(AdditionalAnswers.returnsFirstArg())
    allocatorRunnable.getValue.run()
    scheduler.doKillExecutors(Seq("2"))
    requestExecutorRunnable.getAllValues.asScala.last.run()
    verify(podOperations).delete(secondResolvedPod)
    verify(podOperations, never()).delete(firstResolvedPod)
>>>>>>> master
  }

  test("Executors should be requested in batches.") {
    sparkConf
<<<<<<< HEAD
        .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 1)
        .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 2)
    val scheduler = newSchedulerBackend(true)
=======
      .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 1)
      .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 2)
    val scheduler = newSchedulerBackend()
>>>>>>> master
    scheduler.start()
    requestExecutorRunnable.getValue.run()
    when(podOperations.create(any(classOf[Pod])))
      .thenAnswer(AdditionalAnswers.returnsFirstArg())
<<<<<<< HEAD
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
=======
    val firstResolvedPod = expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
    val secondResolvedPod = expectPodCreationWithId(2, SECOND_EXECUTOR_POD)
    allocatorRunnable.getValue.run()
    verify(podOperations).create(firstResolvedPod)
    verify(podOperations, never()).create(secondResolvedPod)
    val registerFirstExecutorMessage = RegisterExecutor(
      "1", mock[RpcEndpointRef], "localhost", 1, Map.empty[String, String])
    when(taskSchedulerImpl.resourceOffers(any())).thenReturn(Seq.empty)
    driverEndpoint.getValue.receiveAndReply(mock[RpcCallContext])
      .apply(registerFirstExecutorMessage)
    allocatorRunnable.getValue.run()
    verify(podOperations).create(secondResolvedPod)
  }

  test("Scaled down executors should be cleaned up") {
    sparkConf
      .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 1)
      .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 1)
    val scheduler = newSchedulerBackend()
    scheduler.start()

    // The scheduler backend spins up one executor pod.
    requestExecutorRunnable.getValue.run()
    when(podOperations.create(any(classOf[Pod])))
      .thenAnswer(AdditionalAnswers.returnsFirstArg())
    val resolvedPod = expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
>>>>>>> master
    allocatorRunnable.getValue.run()
    val executorEndpointRef = mock[RpcEndpointRef]
    when(executorEndpointRef.address).thenReturn(RpcAddress("pod.example.com", 9000))
    val registerFirstExecutorMessage = RegisterExecutor(
<<<<<<< HEAD
        "1", executorEndpointRef, "localhost:9000", 1, Map.empty[String, String])
    when(taskSchedulerImpl.resourceOffers(any())).thenReturn(Seq.empty)
    driverEndpoint.getValue.receiveAndReply(mock[RpcCallContext])
        .apply(registerFirstExecutorMessage)
=======
      "1", executorEndpointRef, "localhost:9000", 1, Map.empty[String, String])
    when(taskSchedulerImpl.resourceOffers(any())).thenReturn(Seq.empty)
    driverEndpoint.getValue.receiveAndReply(mock[RpcCallContext])
      .apply(registerFirstExecutorMessage)

    // Request that there are 0 executors and trigger deletion from driver.
>>>>>>> master
    scheduler.doRequestTotalExecutors(0)
    requestExecutorRunnable.getAllValues.asScala.last.run()
    scheduler.doKillExecutors(Seq("1"))
    requestExecutorRunnable.getAllValues.asScala.last.run()
<<<<<<< HEAD
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
=======
    verify(podOperations, times(1)).delete(resolvedPod)
    driverEndpoint.getValue.onDisconnected(executorEndpointRef.address)

    val exitedPod = exitPod(resolvedPod, 0)
    executorPodsWatcherArgument.getValue.eventReceived(Action.DELETED, exitedPod)
    allocatorRunnable.getValue.run()

    // No more deletion attempts of the executors.
    // This is graceful termination and should not be detected as a failure.
    verify(podOperations, times(1)).delete(resolvedPod)
    verify(driverEndpointRef, times(1)).send(
      RemoveExecutor("1", ExecutorExited(
        0,
        exitCausedByApp = false,
        s"Container in pod ${exitedPod.getMetadata.getName} exited from" +
          s" explicit termination request.")))
  }

  test("Executors that fail should not be deleted.") {
    sparkConf
      .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 1)
      .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 1)

    val scheduler = newSchedulerBackend()
    scheduler.start()
    val firstResolvedPod = expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
>>>>>>> master
    when(podOperations.create(any(classOf[Pod]))).thenAnswer(AdditionalAnswers.returnsFirstArg())
    requestExecutorRunnable.getValue.run()
    allocatorRunnable.getValue.run()
    val executorEndpointRef = mock[RpcEndpointRef]
    when(executorEndpointRef.address).thenReturn(RpcAddress("pod.example.com", 9000))
    val registerFirstExecutorMessage = RegisterExecutor(
<<<<<<< HEAD
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
=======
      "1", executorEndpointRef, "localhost:9000", 1, Map.empty[String, String])
    when(taskSchedulerImpl.resourceOffers(any())).thenReturn(Seq.empty)
    driverEndpoint.getValue.receiveAndReply(mock[RpcCallContext])
      .apply(registerFirstExecutorMessage)
    driverEndpoint.getValue.onDisconnected(executorEndpointRef.address)
    executorPodsWatcherArgument.getValue.eventReceived(
      Action.ERROR, exitPod(firstResolvedPod, 1))

    // A replacement executor should be created but the error pod should persist.
    val replacementPod = expectPodCreationWithId(2, SECOND_EXECUTOR_POD)
    scheduler.doRequestTotalExecutors(1)
    requestExecutorRunnable.getValue.run()
    allocatorRunnable.getAllValues.asScala.last.run()
    verify(podOperations, never()).delete(firstResolvedPod)
    verify(driverEndpointRef).send(
      RemoveExecutor("1", ExecutorExited(
        1,
        exitCausedByApp = true,
        s"Pod ${FIRST_EXECUTOR_POD.getMetadata.getName}'s executor container exited with" +
          " exit status code 1.")))
  }

  test("Executors disconnected due to unknown reasons are deleted and replaced.") {
    sparkConf
      .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 1)
      .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 1)
    val executorLostReasonCheckMaxAttempts = sparkConf.get(
      KUBERNETES_EXECUTOR_LOST_REASON_CHECK_MAX_ATTEMPTS)

    val scheduler = newSchedulerBackend()
    scheduler.start()
    val firstResolvedPod = expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
>>>>>>> master
    when(podOperations.create(any(classOf[Pod]))).thenAnswer(AdditionalAnswers.returnsFirstArg())
    requestExecutorRunnable.getValue.run()
    allocatorRunnable.getValue.run()
    val executorEndpointRef = mock[RpcEndpointRef]
    when(executorEndpointRef.address).thenReturn(RpcAddress("pod.example.com", 9000))
    val registerFirstExecutorMessage = RegisterExecutor(
<<<<<<< HEAD
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
=======
      "1", executorEndpointRef, "localhost:9000", 1, Map.empty[String, String])
    when(taskSchedulerImpl.resourceOffers(any())).thenReturn(Seq.empty)
    driverEndpoint.getValue.receiveAndReply(mock[RpcCallContext])
      .apply(registerFirstExecutorMessage)

    driverEndpoint.getValue.onDisconnected(executorEndpointRef.address)
    1 to executorLostReasonCheckMaxAttempts foreach { _ =>
      allocatorRunnable.getValue.run()
      verify(podOperations, never()).delete(FIRST_EXECUTOR_POD)
    }

    val recreatedResolvedPod = expectPodCreationWithId(2, SECOND_EXECUTOR_POD)
    allocatorRunnable.getValue.run()
    verify(podOperations).delete(firstResolvedPod)
    verify(driverEndpointRef).send(
      RemoveExecutor("1", SlaveLost("Executor lost for unknown reasons.")))
  }

  test("Executors that fail to start on the Kubernetes API call rebuild in the next batch.") {
    sparkConf
      .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 1)
      .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 1)
    val scheduler = newSchedulerBackend()
    scheduler.start()
    val firstResolvedPod = expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
    when(podOperations.create(firstResolvedPod))
      .thenThrow(new RuntimeException("test"))
    requestExecutorRunnable.getValue.run()
    allocatorRunnable.getValue.run()
    verify(podOperations, times(1)).create(firstResolvedPod)
    val recreatedResolvedPod = expectPodCreationWithId(2, FIRST_EXECUTOR_POD)
    allocatorRunnable.getValue.run()
    verify(podOperations).create(recreatedResolvedPod)
  }

  test("Executors that are initially created but the watch notices them fail are rebuilt" +
    " in the next batch.") {
    sparkConf
      .set(KUBERNETES_ALLOCATION_BATCH_SIZE, 1)
      .set(org.apache.spark.internal.config.EXECUTOR_INSTANCES, 1)
    val scheduler = newSchedulerBackend()
    scheduler.start()
    val firstResolvedPod = expectPodCreationWithId(1, FIRST_EXECUTOR_POD)
    when(podOperations.create(FIRST_EXECUTOR_POD)).thenAnswer(AdditionalAnswers.returnsFirstArg())
    requestExecutorRunnable.getValue.run()
    allocatorRunnable.getValue.run()
    verify(podOperations, times(1)).create(firstResolvedPod)
    executorPodsWatcherArgument.getValue.eventReceived(Action.ERROR, firstResolvedPod)
    val recreatedResolvedPod = expectPodCreationWithId(2, FIRST_EXECUTOR_POD)
    allocatorRunnable.getValue.run()
    verify(podOperations).create(recreatedResolvedPod)
  }

  private def newSchedulerBackend(): KubernetesClusterSchedulerBackend = {
    new KubernetesClusterSchedulerBackend(
      taskSchedulerImpl,
      rpcEnv,
      executorPodFactory,
      kubernetesClient,
      allocatorExecutor,
      requestExecutorsService) {

      override def applicationId(): String = APP_ID
    }
  }

  private def exitPod(basePod: Pod, exitCode: Int): Pod = {
    new PodBuilder(basePod)
>>>>>>> master
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

<<<<<<< HEAD
  private def expectPodCreationWithId(executorId: Int, expectedPod: Pod): Unit = {
    when(executorPodFactory.createExecutorPod(
        executorId.toString,
        APP_ID,
        DRIVER_URL,
        sparkConf.getExecutorEnv,
        driverPod,
        Map.empty)).thenReturn(expectedPod)
  }

=======
  private def expectPodCreationWithId(executorId: Int, expectedPod: Pod): Pod = {
    val resolvedPod = new PodBuilder(expectedPod)
      .editMetadata()
        .addToLabels(SPARK_EXECUTOR_ID_LABEL, executorId.toString)
        .endMetadata()
      .build()
    when(executorPodFactory.createExecutorPod(
      executorId.toString,
      APP_ID,
      DRIVER_URL,
      sparkConf.getExecutorEnv,
      driverPod,
      Map.empty)).thenReturn(resolvedPod)
    resolvedPod
  }
>>>>>>> master
}
