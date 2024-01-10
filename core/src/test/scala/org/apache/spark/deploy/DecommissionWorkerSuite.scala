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

package org.apache.spark.deploy

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.deploy.DeployMessages.{DecommissionWorkers, MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.master.{ApplicationInfo, Master, WorkerInfo}
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ExternalBlockHandler
import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

class DecommissionWorkerSuite
  extends SparkFunSuite
    with Logging
    with LocalSparkContext
    with BeforeAndAfterEach {

  private var masterAndWorkerConf: SparkConf = null
  private var masterAndWorkerSecurityManager: SecurityManager = null
  private var masterRpcEnv: RpcEnv = null
  private var master: Master = null
  private var workerIdToRpcEnvs: mutable.HashMap[String, RpcEnv] = null
  private var workers: mutable.ArrayBuffer[Worker] = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    masterAndWorkerConf = new SparkConf()
      .set(config.DECOMMISSION_ENABLED, true)
    masterAndWorkerSecurityManager = new SecurityManager(masterAndWorkerConf)
    masterRpcEnv = RpcEnv.create(
      Master.SYSTEM_NAME,
      "localhost",
      0,
      masterAndWorkerConf,
      masterAndWorkerSecurityManager)
    master = makeMaster()
    workerIdToRpcEnvs = mutable.HashMap.empty
    workers = mutable.ArrayBuffer.empty
  }

  override def afterEach(): Unit = {
    try {
      masterRpcEnv.shutdown()
      workerIdToRpcEnvs.values.foreach(_.shutdown())
      workerIdToRpcEnvs.clear()
      master.stop()
      workers.foreach(_.stop())
      workers.clear()
      masterRpcEnv = null
    } finally {
      super.afterEach()
    }
  }

  // Unlike TestUtils.withListener, it also waits for the job to be done
  def withListener(sc: SparkContext, listener: RootStageAwareListener)
                  (body: SparkListener => Unit): Unit = {
    sc.addSparkListener(listener)
    try {
      body(listener)
      sc.listenerBus.waitUntilEmpty()
      listener.waitForJobDone()
    } finally {
      sc.listenerBus.removeListener(listener)
    }
  }

  test("decommission workers should not result in job failure") {
    val maxTaskFailures = 2
    val numTimesToKillWorkers = maxTaskFailures + 1
    val numWorkers = numTimesToKillWorkers + 1
    createWorkers(numWorkers)

    // Here we will have a single task job and we will keep decommissioning (and killing) the
    // worker running that task K times. Where K is more than the maxTaskFailures. Since the worker
    // is notified of the decommissioning, the task failures can be ignored and not fail
    // the job.

    sc = createSparkContext(config.TASK_MAX_FAILURES.key -> maxTaskFailures.toString)
    val executorIdToWorkerInfo = getExecutorToWorkerAssignments
    val taskIdsKilled = new ConcurrentHashMap[Long, Boolean]
    val listener = new RootStageAwareListener {
      override def handleRootTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        val taskInfo = taskStart.taskInfo
        if (taskIdsKilled.size() < numTimesToKillWorkers) {
          val workerInfo = executorIdToWorkerInfo(taskInfo.executorId)
          decommissionWorkerOnMaster(workerInfo, "partition 0 must die")
          killWorkerAfterTimeout(workerInfo, 1)
          taskIdsKilled.put(taskInfo.taskId, true)
        }
      }
    }
    withListener(sc, listener) { _ =>
      val jobResult = sc.parallelize(1 to 1, 1).map { _ =>
        Thread.sleep(5 * 1000L); 1
      }.count()
      assert(jobResult === 1)
    }
    // single task job that gets to run numTimesToKillWorkers + 1 times.
    assert(listener.getTasksFinished().size === numTimesToKillWorkers + 1)
    listener.rootTasksStarted.asScala.foreach { taskInfo =>
      assert(taskInfo.index == 0, s"Unknown task index ${taskInfo.index}")
    }
    listener.rootTasksEnded.asScala.foreach { taskInfo =>
      assert(taskInfo.index === 0, s"Expected task index ${taskInfo.index} to be 0")
      // If a task has been killed then it shouldn't be successful
      val taskSuccessExpected = !taskIdsKilled.getOrDefault(taskInfo.taskId, false)
      val taskSuccessActual = taskInfo.successful
      assert(taskSuccessActual === taskSuccessExpected,
        s"Expected task success $taskSuccessActual == $taskSuccessExpected")
    }
  }

  test("decommission workers ensure that shuffle output is regenerated even with shuffle service") {
    createWorkers(2)
    val ss = new ExternalShuffleServiceHolder()

    sc = createSparkContext(
      config.Tests.TEST_NO_STAGE_RETRY.key -> "true",
      config.SHUFFLE_MANAGER.key -> "sort",
      config.SHUFFLE_SERVICE_ENABLED.key -> "true",
      config.SHUFFLE_SERVICE_PORT.key -> ss.getPort.toString
    )
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

    // Here we will create a 2 stage job: The first stage will have two tasks and the second stage
    // will have one task. The two tasks in the first stage will be long and short. We decommission
    // and kill the worker after the short task is done. Eventually the driver should get the
    // executor lost signal for the short task executor. This should trigger regenerating
    // the shuffle output since we cleanly decommissioned the executor, despite running with an
    // external shuffle service.
    try {
      val executorIdToWorkerInfo = getExecutorToWorkerAssignments
      val workerForTask0Decommissioned = new AtomicBoolean(false)
      // single task job
      val listener = new RootStageAwareListener {
        override def handleRootTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          val taskInfo = taskEnd.taskInfo
          if (taskInfo.index == 0) {
            if (workerForTask0Decommissioned.compareAndSet(false, true)) {
              val workerInfo = executorIdToWorkerInfo(taskInfo.executorId)
              decommissionWorkerOnMaster(workerInfo, "Kill early done map worker")
              killWorkerAfterTimeout(workerInfo, 0)
              logInfo(s"Killed the node ${workerInfo.hostPort} that was running the early task")
            }
          }
        }
      }
      withListener(sc, listener) { _ =>
        val jobResult = sc.parallelize(1 to 2, 2).mapPartitionsWithIndex((pid, _) => {
          val sleepTimeSeconds = if (pid == 0) 1 else 10
          Thread.sleep(sleepTimeSeconds * 1000L)
          List(1).iterator
        }, preservesPartitioning = true).repartition(1).sum()
        assert(jobResult === 2)
      }
      val tasksSeen = listener.getTasksFinished()
      // 4 tasks: 2 from first stage, one retry due to decom, one more from the second stage.
      assert(tasksSeen.size === 4, s"Expected 4 tasks but got $tasksSeen")
      listener.rootTasksStarted.asScala.foreach { taskInfo =>
        assert(taskInfo.index <= 1, s"Expected ${taskInfo.index} <= 1")
        assert(taskInfo.successful, s"Task ${taskInfo.index} should be successful")
      }
      val tasksEnded = listener.rootTasksEnded.asScala
      tasksEnded.filter(_.index != 0).foreach { taskInfo =>
        assert(taskInfo.attemptNumber === 0, "2nd task should succeed on 1st attempt")
      }
      val firstTaskAttempts = tasksEnded.filter(_.index == 0)
      assert(firstTaskAttempts.size > 1, s"Task 0 should have multiple attempts")
    } finally {
      ss.close()
    }
  }

  def testFetchFailures(initialSleepMillis: Int): Unit = {
    createWorkers(2)
    sc = createSparkContext(
      config.Tests.TEST_NO_STAGE_RETRY.key -> "false",
      "spark.test.executor.decommission.initial.sleep.millis" -> initialSleepMillis.toString,
      config.UNREGISTER_OUTPUT_ON_HOST_ON_FETCH_FAILURE.key -> "true")
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

    val executorIdToWorkerInfo = getExecutorToWorkerAssignments
    val executorToDecom = executorIdToWorkerInfo.keysIterator.next()

    // The task code below cannot call executorIdToWorkerInfo, so we need to pre-compute
    // the worker to decom to force it to be serialized into the task.
    val workerToDecom = executorIdToWorkerInfo(executorToDecom)

    // The setup of this job is similar to the one above: 2 stage job with first stage having
    // long and short tasks. Except that we want the shuffle output to be regenerated on a
    // fetch failure instead of an executor lost. Since it is hard to "trigger a fetch failure",
    // we manually raise the FetchFailed exception when the 2nd stage's task runs and require that
    // fetch failure to trigger a recomputation.
    logInfo(s"Will try to decommission the task running on executor $executorToDecom")
    val listener = new RootStageAwareListener {
      override def handleRootTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val taskInfo = taskEnd.taskInfo
        if (taskInfo.executorId == executorToDecom && taskInfo.attemptNumber == 0 &&
          taskEnd.stageAttemptId == 0 && taskEnd.stageId == 0) {
          decommissionWorkerOnMaster(workerToDecom,
            "decommission worker after task on it is done")
        }
      }
    }
    withListener(sc, listener) { _ =>
      val jobResult = sc.parallelize(1 to 2, 2).mapPartitionsWithIndex((_, _) => {
        val executorId = SparkEnv.get.executorId
        val context = TaskContext.get()
        // Only sleep in the first attempt to create the required window for decommissioning.
        // Subsequent attempts don't need to be delayed to speed up the test.
        if (context.attemptNumber() == 0 && context.stageAttemptNumber() == 0) {
          val sleepTimeSeconds = if (executorId == executorToDecom) 10 else 1
          Thread.sleep(sleepTimeSeconds * 1000L)
        }
        List(1).iterator
      }, preservesPartitioning = true)
        .repartition(1).mapPartitions(iter => {
        val context = TaskContext.get()
        if (context.attemptNumber() == 0 && context.stageAttemptNumber() == 0) {
          // Wait a bit for the decommissioning to be triggered in the listener
          Thread.sleep(5000)
          // MapIndex is explicitly -1 to force the entire host to be decommissioned
          // However, this will cause both the tasks in the preceding stage since the host here is
          // "localhost" (shortcoming of this single-machine unit test in that all the workers
          // are actually on the same host)
          throw new FetchFailedException(BlockManagerId(executorToDecom,
            workerToDecom.host, workerToDecom.port), 0, 0, -1, 0, "Forcing fetch failure")
        }
        val sumVal: List[Int] = List(iter.sum)
        sumVal.iterator
      }, preservesPartitioning = true)
        .sum()
      assert(jobResult === 2)
    }
    // 6 tasks: 2 from first stage, 2 rerun again from first stage, 2nd stage attempt 1 and 2.
    val tasksSeen = listener.getTasksFinished()
    assert(tasksSeen.size === 6, s"Expected 6 tasks but got $tasksSeen")
  }

  test("decommission stalled workers ensure that fetch failures lead to rerun") {
    testFetchFailures(3600 * 1000)
  }

  test("decommission eager workers ensure that fetch failures lead to rerun") {
    testFetchFailures(0)
  }

  private abstract class RootStageAwareListener extends SparkListener {
    private var rootStageId: Option[Int] = None
    private val tasksFinished = new ConcurrentLinkedQueue[String]()
    private val jobDone = new AtomicBoolean(false)
    val rootTasksStarted = new ConcurrentLinkedQueue[TaskInfo]()
    val rootTasksEnded = new ConcurrentLinkedQueue[TaskInfo]()

    protected def isRootStageId(stageId: Int): Boolean =
      (rootStageId.isDefined && rootStageId.get == stageId)

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      if (stageSubmitted.stageInfo.parentIds.isEmpty && rootStageId.isEmpty) {
        rootStageId = Some(stageSubmitted.stageInfo.stageId)
      }
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      jobEnd.jobResult match {
        case JobSucceeded => jobDone.set(true)
        case JobFailed(exception) => logError(s"Job failed", exception)
      }
    }

    protected def handleRootTaskEnd(end: SparkListenerTaskEnd) = {}

    protected def handleRootTaskStart(start: SparkListenerTaskStart) = {}

    private def getSignature(taskInfo: TaskInfo, stageId: Int, stageAttemptId: Int):
    String = {
      s"${stageId}:${stageAttemptId}:" +
        s"${taskInfo.index}:${taskInfo.attemptNumber}-${taskInfo.status}"
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
      val signature = getSignature(taskStart.taskInfo, taskStart.stageId, taskStart.stageAttemptId)
      logInfo(s"Task started: $signature")
      if (isRootStageId(taskStart.stageId)) {
        rootTasksStarted.add(taskStart.taskInfo)
        handleRootTaskStart(taskStart)
      }
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      // Task resubmit is a signal to DAGScheduler not a real task end event. Ignore it here
      // to avoid over count.
      if (taskEnd.reason == Resubmitted) return
      val taskSignature = getSignature(taskEnd.taskInfo, taskEnd.stageId, taskEnd.stageAttemptId)
      logInfo(s"Task End $taskSignature")
      tasksFinished.add(taskSignature)
      if (isRootStageId(taskEnd.stageId)) {
        rootTasksEnded.add(taskEnd.taskInfo)
        handleRootTaskEnd(taskEnd)
      }
    }

    def getTasksFinished(): Seq[String] = {
      tasksFinished.asScala.toList
    }

    def waitForJobDone(): Unit = {
      eventually(timeout(10.seconds), interval(100.milliseconds)) {
        assert(jobDone.get(), "Job isn't successfully done yet")
      }
    }
  }

  private def getExecutorToWorkerAssignments: Map[String, WorkerInfo] = {
    val executorIdToWorkerInfo = mutable.HashMap[String, WorkerInfo]()
    master.workers.foreach { wi =>
      assert(wi.executors.size <= 1, "There should be at most one executor per worker")
      // Cast the executorId to string since the TaskInfo.executorId is a string
      wi.executors.values.foreach { e =>
        val executorIdString = e.id.toString
        val oldWorkerInfo = executorIdToWorkerInfo.put(executorIdString, wi)
        assert(oldWorkerInfo.isEmpty,
          s"Executor $executorIdString already present on another worker ${oldWorkerInfo}")
      }
    }
    executorIdToWorkerInfo.toMap
  }

  private def makeMaster(): Master = {
    val master = new Master(
      masterRpcEnv,
      masterRpcEnv.address,
      0,
      masterAndWorkerSecurityManager,
      masterAndWorkerConf)
    masterRpcEnv.setupEndpoint(Master.ENDPOINT_NAME, master)
    master
  }

  private def createWorkers(numWorkers: Int, cores: Int = 1, memory: Int = 1024): Unit = {
    val workerRpcEnvs = (0 until numWorkers).map { i =>
      RpcEnv.create(
        Worker.SYSTEM_NAME + i,
        "localhost",
        0,
        masterAndWorkerConf,
        masterAndWorkerSecurityManager)
    }
    workers.clear()
    val rpcAddressToRpcEnv: mutable.HashMap[RpcAddress, RpcEnv] = mutable.HashMap.empty
    workerRpcEnvs.foreach { rpcEnv =>
      val workDir = Utils.createTempDir(namePrefix = this.getClass.getSimpleName()).toString
      val worker = new Worker(rpcEnv, 0, cores, memory, Array(masterRpcEnv.address),
        Worker.ENDPOINT_NAME, workDir, masterAndWorkerConf, masterAndWorkerSecurityManager)
      rpcEnv.setupEndpoint(Worker.ENDPOINT_NAME, worker)
      workers.append(worker)
      val oldRpcEnv = rpcAddressToRpcEnv.put(rpcEnv.address, rpcEnv)
      logInfo(s"Created a worker at ${rpcEnv.address} with workdir $workDir")
      assert(oldRpcEnv.isEmpty, s"Detected duplicate rpcEnv ${oldRpcEnv} for ${rpcEnv.address}")
    }
    workerIdToRpcEnvs.clear()
    // Wait until all workers register with master successfully
    eventually(timeout(1.minute), interval(1.seconds)) {
      val workersOnMaster = getMasterState.workers
      val numWorkersCurrently = workersOnMaster.length
      logInfo(s"Waiting for $numWorkers workers to come up: So far $numWorkersCurrently")
      assert(numWorkersCurrently === numWorkers)
      workersOnMaster.foreach { workerInfo =>
        val rpcAddress = RpcAddress(workerInfo.host, workerInfo.port)
        val rpcEnv = rpcAddressToRpcEnv(rpcAddress)
        assert(rpcEnv != null, s"Cannot find the worker for $rpcAddress")
        val oldRpcEnv = workerIdToRpcEnvs.put(workerInfo.id, rpcEnv)
        assert(oldRpcEnv.isEmpty, s"Detected duplicate rpcEnv ${oldRpcEnv} for worker " +
          s"${workerInfo.id}")
      }
    }
    logInfo(s"Created ${workers.size} workers")
  }

  private def getMasterState: MasterStateResponse = {
    master.self.askSync[MasterStateResponse](RequestMasterState)
  }

  private def getApplications(): Array[ApplicationInfo] = {
    getMasterState.activeApps
  }

  def decommissionWorkerOnMaster(workerInfo: WorkerInfo, reason: String): Unit = {
    logInfo(s"Trying to decommission worker ${workerInfo.id} for reason `$reason`")
    master.self.send(DecommissionWorkers(Seq(workerInfo.id)))
  }

  def killWorkerAfterTimeout(workerInfo: WorkerInfo, secondsToWait: Int): Unit = {
    val env = workerIdToRpcEnvs(workerInfo.id)
    Thread.sleep(secondsToWait * 1000L)
    env.shutdown()
    env.awaitTermination()
  }

  def createSparkContext(extraConfs: (String, String)*): SparkContext = {
    val conf = new SparkConf()
      .setMaster(masterRpcEnv.address.toSparkURL)
      .setAppName("test")
      .setAll(extraConfs)
    sc = new SparkContext(conf)
    val appId = sc.applicationId
    eventually(timeout(1.minute), interval(1.seconds)) {
      val apps = getApplications()
      assert(apps.length === 1)
      assert(apps.head.id === appId)
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    sc
  }

  private class ExternalShuffleServiceHolder() {
    // The external shuffle service can start with default configs and not get polluted by the
    // other configs used in this test.
    private val transportConf = SparkTransportConf.fromSparkConf(new SparkConf(),
      "shuffle", numUsableCores = 2)
    private val rpcHandler = new ExternalBlockHandler(transportConf, null)
    private val transportContext = new TransportContext(transportConf, rpcHandler)
    private val server = transportContext.createServer()

    def getPort: Int = server.getPort

    def close(): Unit = {
      Utils.tryLogNonFatalError {
        server.close()
      }
      Utils.tryLogNonFatalError {
        rpcHandler.close()
      }
      Utils.tryLogNonFatalError {
        transportContext.close()
      }
    }
  }
}
