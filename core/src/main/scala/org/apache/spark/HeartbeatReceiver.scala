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

package org.apache.spark

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
import scala.concurrent.Future

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config.Network
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, StandaloneSchedulerBackend}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RemoveExecutor
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util._

/**
 * A heartbeat from executors to the driver. This is a shared message used by several internal
 * components to convey liveness or execution information for in-progress tasks. It will also
 * expire the hosts that have not heartbeated for more than spark.network.timeout.
 * spark.executor.heartbeatInterval should be significantly less than spark.network.timeout.
 */
private[spark] case class Heartbeat(
    executorId: String,
    // taskId -> accumulator updates
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
    blockManagerId: BlockManagerId,
    // (stageId, stageAttemptId) -> executor metric peaks
    executorUpdates: Map[(Int, Int), ExecutorMetrics])

/**
 * An event that SparkContext uses to notify HeartbeatReceiver that SparkContext.taskScheduler is
 * created.
 */
private[spark] case object TaskSchedulerIsSet

private[spark] case object ExpireDeadHosts

private case class ExecutorRegistered(executorId: String)

private case class ExecutorRemoved(executorId: String)

private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
 * Lives in the driver to receive heartbeats from executors..
 */
private[spark] class HeartbeatReceiver(sc: SparkContext, clock: Clock)
  extends SparkListener with ThreadSafeRpcEndpoint with Logging {

  def this(sc: SparkContext) = {
    this(sc, new SystemClock)
  }

  sc.listenerBus.addToManagementQueue(this)

  override val rpcEnv: RpcEnv = sc.env.rpcEnv

  private[spark] var scheduler: TaskScheduler = null

  /**
   * [SPARK-39984]
   * Please make sure the intersection between `executorLastSeen` and `executorExpiryCandidates` is
   * an empty set. If the intersection is not empty, it is possible to never kill the executor until
   * the executor recovers. When an executor is in both `executorLastSeen` and
   * `executorExpiryCandidates`, the value of `workerLastHeartbeat` in `executorExpiryCandidates`
   * may update if the worker sends heartbeats to master normally.
   *
   * `executorLastSeen`:
   *  - key: executor ID
   *  - value: timestamp of when the last heartbeat from this executor was received
   *
   * `executorExpiryCandidates`: executor ID -> WorkerLastHeartbeat
   *  - key: executor ID
   *  - value: timestamp of when the last heartbeat from the worker was received
   *
   * when driver does not receive any heartbeat from an executor for `executorTimeoutMs` seconds,
   * the driver will ask master for the last heartbeat from the worker which the executor is running
   * on.
   */
  private val executorLastSeen = new HashMap[String, Long]
  private val executorExpiryCandidates = new HashMap[String, Long]

  private val executorTimeoutMs = sc.conf.get(
    config.STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT
  ).getOrElse(
    sc.conf.get(Network.NETWORK_EXECUTOR_TIMEOUT)
  )

  private val checkTimeoutIntervalMs = sc.conf.get(Network.NETWORK_TIMEOUT_INTERVAL)

  private val executorHeartbeatIntervalMs = sc.conf.get(config.EXECUTOR_HEARTBEAT_INTERVAL)

  /**
   * Currently, [SPARK-39984] is only for StandaloneSchedulerBackend.
   *
   * `checkWorkerLastHeartbeat`: A flag to enable two-phase executor timeout.
   * `expiryCandidatesTimeout`: The timeout used for executorExpiryCandidates.
   */
  private lazy val checkWorkerLastHeartbeat = sc.schedulerBackend match {
    case _: CoarseGrainedSchedulerBackend =>
      sc.conf.get(config.HEARTBEAT_RECEIVER_CHECK_WORKER_LAST_HEARTBEAT) &&
        sc.schedulerBackend.isInstanceOf[StandaloneSchedulerBackend]
    case _: LocalSchedulerBackend => false
    case other => throw new UnsupportedOperationException(
      s"Unknown scheduler backend: ${other.getClass}")
  }

  private lazy val expiryCandidatesTimeout = checkWorkerLastHeartbeat match {
    case true =>
      logWarning(s"Worker heartbeat check is enabled. It only works normally when" +
        s"${config.HEARTBEAT_EXPIRY_CANDIDATES_TIMEOUT.key} is larger than worker's" +
        s"heartbeat interval.")
      sc.conf.get(config.HEARTBEAT_EXPIRY_CANDIDATES_TIMEOUT)
    case false => 0
  }

  require(checkTimeoutIntervalMs <= executorTimeoutMs,
    s"${Network.NETWORK_TIMEOUT_INTERVAL.key} should be less than or " +
      s"equal to ${config.STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT.key}.")
  require(executorHeartbeatIntervalMs <= executorTimeoutMs,
    s"${config.EXECUTOR_HEARTBEAT_INTERVAL.key} should be less than or " +
      s"equal to ${config.STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT.key}")

  private var timeoutCheckingTask: ScheduledFuture[_] = null

  // "eventLoopThread" is used to run some pretty fast actions. The actions running in it should not
  // block the thread for a long time.
  private val eventLoopThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartbeat-receiver-event-loop-thread")

  private val killExecutorThread = ThreadUtils.newDaemonSingleThreadExecutor("kill-executor-thread")

  override def onStart(): Unit = {
    timeoutCheckingTask = eventLoopThread.scheduleAtFixedRate(
      () => Utils.tryLogNonFatalError { Option(self).foreach(_.ask[Boolean](ExpireDeadHosts)) },
      executorTimeoutMs, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    // Messages sent and received locally
    case ExecutorRegistered(executorId) =>
      executorLastSeen(executorId) = clock.getTimeMillis()
      removeExecutorFromExpiryCandidates(executorId)
      context.reply(true)
    case ExecutorRemoved(executorId) =>
      executorLastSeen.remove(executorId)
      removeExecutorFromExpiryCandidates(executorId)
      context.reply(true)
    case TaskSchedulerIsSet =>
      scheduler = sc.taskScheduler
      context.reply(true)
    case ExpireDeadHosts =>
      expireDeadHosts()
      context.reply(true)

    // Messages received from executors
    case heartbeat @ Heartbeat(executorId, accumUpdates, blockManagerId, executorUpdates) =>
      var reregisterBlockManager = !sc.isStopped
      if (scheduler != null) {
        if (executorLastSeen.contains(executorId) ||
          (checkWorkerLastHeartbeat && executorExpiryCandidates.contains(executorId))) {
          executorLastSeen(executorId) = clock.getTimeMillis()
          removeExecutorFromExpiryCandidates(executorId)
          eventLoopThread.submit(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, accumUpdates, blockManagerId, executorUpdates)
              reregisterBlockManager &= unknownExecutor
              val response = HeartbeatResponse(reregisterBlockManager)
              context.reply(response)
            }
          })
        } else {
          // This may happen if we get an executor's in-flight heartbeat immediately
          // after we just removed it. It's not really an error condition so we should
          // not log warning here. Otherwise there may be a lot of noise especially if
          // we explicitly remove executors (SPARK-4134).
          logDebug(s"Received heartbeat from unknown executor $executorId")
          context.reply(HeartbeatResponse(reregisterBlockManager))
        }
      } else {
        // Because Executor will sleep several seconds before sending the first "Heartbeat", this
        // case rarely happens. However, if it really happens, log it and ask the executor to
        // register itself again.
        logWarning(s"Dropping $heartbeat because TaskScheduler is not ready yet")
        context.reply(HeartbeatResponse(reregisterBlockManager))
      }
  }

  /**
   * Send ExecutorRegistered to the event loop to add a new executor. Only for test.
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
   */
  def addExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRegistered(executorId)))
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor registrations.
   */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    addExecutor(executorAdded.executorId)
  }

  /**
   * Send ExecutorRemoved to the event loop to remove an executor. Only for test.
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
   */
  def removeExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRemoved(executorId)))
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor removals so it doesn't
   * log superfluous errors.
   *
   * Note that we must do this after the executor is actually removed to guard against the
   * following race condition: if we remove an executor's metadata from our data structure
   * prematurely, we may get an in-flight heartbeat from the executor before the executor is
   * actually removed, in which case we will still mark the executor as a dead host later
   * and expire it with loud error messages.
   */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    removeExecutor(executorRemoved.executorId)
  }

  private def killExecutor(executorId: String, timeout: Long): Unit = {
    logWarning(s"Removing executor $executorId with no recent heartbeats: " +
      s"${timeout} ms exceeds timeout $executorTimeoutMs ms")
    killExecutorThread.submit(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        // Note: we want to get an executor back after expiring this one,
        // so do not simply call `sc.killExecutor` here (SPARK-8119)
        sc.killAndReplaceExecutor(executorId)
        // SPARK-27348: in case of the executors which are not gracefully shut down,
        // we should remove lost executors from CoarseGrainedSchedulerBackend manually
        // here to guarantee two things:
        // 1) explicitly remove executor information from CoarseGrainedSchedulerBackend for
        //    a lost executor instead of waiting for disconnect message
        // 2) call scheduler.executorLost() underlying to fail any tasks assigned to
        //    those executors to avoid app hang
        sc.schedulerBackend match {
          case backend: CoarseGrainedSchedulerBackend =>
            // TODO (SPARK-39984): Update causedByApp when we have a hanging task detector
            backend.driverEndpoint.send(RemoveExecutor(executorId,
              ExecutorProcessLost(
                s"Executor heartbeat timed out after ${timeout} ms")))
          // LocalSchedulerBackend is used locally and only has one single executor
          case _: LocalSchedulerBackend =>

          case other => throw new UnsupportedOperationException(
            s"Unknown scheduler backend: ${other.getClass}")
        }
      }
    })
  }

  private def removeExecutorFromExpiryCandidates(executorId: String): Unit = {
    if (checkWorkerLastHeartbeat) {
      executorExpiryCandidates.remove(executorId)
    }
  }

  private def expireDeadHosts(): Unit = {
  /**
   * [SPARK-39984]
   * The driver’s HeartbeatReceiver will expire an executor if it does not receive any heartbeat
   * from the executor for `executorTimeoutMs` (default 120s) seconds. However, lowering from 120
   * seconds has other challenges. For example: when executor is performing full GC, it cannot
   * send/reply any message for tens of seconds (based on your environment). Hence,
   * HeartbeatReceiver cannot whether the heartbeat loss is caused by network issues or other
   * reasons (e.g. full GC). To address this, we designed a new Heartbeat Receiver mechanism for
   * standalone deployments.
   *
   * For standalone deployments:
   * If driver does not receive any heartbeat from the executor for `executorTimeoutMs` seconds,
   * HeartbeatReceiver will send a request to master to ask for the latest heartbeat from the
   * worker which the executor runs on. HeartbeatReceiver can determine whether the heartbeat loss
   * is caused by network issues or other issues (e.g. GC). If the heartbeat loss is not caused by
   * network issues, the HeartbeatReceiver will put the executor into `executorExpiryCandidates`
   * rather than expiring it immediately.
   */
    logTrace("Checking for hosts with no recent heartbeats in HeartbeatReceiver.")
    val now = clock.getTimeMillis()
    if (!checkWorkerLastHeartbeat) {
      for ((executorId, lastSeenMs) <- executorLastSeen) {
        if (now - lastSeenMs > executorTimeoutMs) {
          killExecutor(executorId, now - lastSeenMs)
          executorLastSeen.remove(executorId)
        }
      }
    } else {
      val buf = new ArrayBuffer[String]()
      for ((executorId, lastSeenMs) <- executorLastSeen) {
        if (now - lastSeenMs > executorTimeoutMs) {
            buf += executorId
        }
      }

      sc.schedulerBackend match {
        case backend: StandaloneSchedulerBackend =>
          backend.client.workerLastHeartbeat(sc.applicationId, buf) match {
            case Some(workerLastHeartbeats) =>
              for ((executorId, workerLastHeartbeat) <- buf zip workerLastHeartbeats) {
                executorExpiryCandidates(executorId) = workerLastHeartbeat
                executorLastSeen.remove(executorId)
              }
            case None =>
              for (executorId <- buf) {
                val lastSeenMs = executorLastSeen(executorId)
                executorExpiryCandidates(executorId) = lastSeenMs
                executorLastSeen.remove(executorId)
              }
          }
        case _ =>
      }

      for ((executorId, workerLastHeartbeat) <- executorExpiryCandidates) {
        if (now - workerLastHeartbeat > expiryCandidatesTimeout) {
          killExecutor(executorId, now - workerLastHeartbeat)
          executorExpiryCandidates.remove(executorId)
          executorLastSeen.remove(executorId)
        }
      }
    }
  }

  override def onStop(): Unit = {
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel(true)
    }
    eventLoopThread.shutdownNow()
    killExecutorThread.shutdownNow()
  }
}


private[spark] object HeartbeatReceiver {
  val ENDPOINT_NAME = "HeartbeatReceiver"
}
