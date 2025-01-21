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

import java.io.Closeable
import java.util.{Properties, TimerTask}
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success => ScalaSuccess, Try}

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.{Logging, MDC, MessageWithContext}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.{RpcEndpointRef, RpcTimeout}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util._

/**
 * :: Experimental ::
 * A [[TaskContext]] with extra contextual info and tooling for tasks in a barrier stage.
 * Use [[BarrierTaskContext#get]] to obtain the barrier context for a running barrier task.
 */
@Experimental
@Since("2.4.0")
class BarrierTaskContext private[spark] (
    taskContext: TaskContext) extends TaskContext with Logging {

  import BarrierTaskContext._

  // Find the driver side RPCEndpointRef of the coordinator that handles all the barrier() calls.
  private val barrierCoordinator: RpcEndpointRef = {
    val env = SparkEnv.get
    RpcUtils.makeDriverRef("barrierSync", env.conf, env.rpcEnv)
  }

  // Local barrierEpoch that identify a barrier() call from current task, it shall be identical
  // with the driver side epoch.
  private var barrierEpoch = 0

  private def logProgressInfo(msg: MessageWithContext, startTime: Option[Long]): Unit = {
    val waitMsg = startTime.fold(log"")(st => log", waited " +
      log"for ${MDC(TOTAL_TIME, System.currentTimeMillis() - st)} ms,")
    logInfo(log"Task ${MDC(TASK_ATTEMPT_ID, taskAttemptId())}" +
      log" from Stage ${MDC(STAGE_ID, stageId())}" +
      log"(Attempt ${MDC(STAGE_ATTEMPT_ID, stageAttemptNumber())}) " +
      msg + waitMsg +
      log" current barrier epoch is ${MDC(BARRIER_EPOCH, barrierEpoch)}.")
  }

  private def runBarrier(message: String, requestMethod: RequestMethod.Value): Array[String] = {
    logProgressInfo(log"has entered the global sync", None)
    logTrace("Current callSite: " + Utils.getCallSite())

    val startTime = System.currentTimeMillis()
    val timerTask = new TimerTask {
      override def run(): Unit = {
        logProgressInfo(
          log"waiting under the global sync since ${MDC(TIME, startTime)}",
          Some(startTime)
        )
      }
    }
    // Log the update of global sync every 1 minute.
    timer.scheduleAtFixedRate(timerTask, 1, 1, TimeUnit.MINUTES)

    try {
      val abortableRpcFuture = barrierCoordinator.askAbortable[Array[String]](
        message = RequestToSync(numPartitions(), stageId(), stageAttemptNumber(), taskAttemptId(),
          barrierEpoch, partitionId(), message, requestMethod),
        // Set a fixed timeout for RPC here, so users shall get a SparkException thrown by
        // BarrierCoordinator on timeout, instead of RPCTimeoutException from the RPC framework.
        timeout = new RpcTimeout(365.days, "barrierTimeout"))

      // Wait the RPC future to be completed, but every 1 second it will jump out waiting
      // and check whether current spark task is killed. If killed, then throw
      // a `TaskKilledException`, otherwise continue wait RPC until it completes.

      while (!abortableRpcFuture.future.isCompleted) {
        try {
          // wait RPC future for at most 1 second
          Thread.sleep(1000)
        } catch {
          case _: InterruptedException => // task is killed by driver
        } finally {
          Try(taskContext.killTaskIfInterrupted()) match {
            case ScalaSuccess(_) => // task is still running healthily
            case Failure(e) => abortableRpcFuture.abort(e)
          }
        }
      }
      // messages which consist of all barrier tasks' messages. The future will return the
      // desired messages if it is completed successfully. Otherwise, exception could be thrown.
      val messages = abortableRpcFuture.future.value.get.get

      barrierEpoch += 1
      logProgressInfo(log"finished global sync successfully", Some(startTime))
      messages
    } catch {
      case e: SparkException =>
        logProgressInfo(log"failed to perform global sync", Some(startTime))
        throw e
    } finally {
      timerTask.cancel()
      timer.purge()
    }
  }

  /**
   * :: Experimental ::
   * Sets a global barrier and waits until all tasks in this stage hit this barrier. Similar to
   * MPI_Barrier function in MPI, the barrier() function call blocks until all tasks in the same
   * stage have reached this routine.
   *
   * CAUTION! In a barrier stage, each task must have the same number of barrier() calls, in all
   * possible code branches. Otherwise, you may get the job hanging or a SparkException after
   * timeout. Some examples of '''misuses''' are listed below:
   * 1. Only call barrier() function on a subset of all the tasks in the same barrier stage, it
   * shall lead to timeout of the function call.
   * {{{
   *   rdd.barrier().mapPartitions { iter =>
   *       val context = BarrierTaskContext.get()
   *       if (context.partitionId() == 0) {
   *           // Do nothing.
   *       } else {
   *           context.barrier()
   *       }
   *       iter
   *   }
   * }}}
   *
   * 2. Include barrier() function in a try-catch code block, this may lead to timeout of the
   * second function call.
   * {{{
   *   rdd.barrier().mapPartitions { iter =>
   *       val context = BarrierTaskContext.get()
   *       try {
   *           // Do something that might throw an Exception.
   *           doSomething()
   *           context.barrier()
   *       } catch {
   *           case e: Exception => logWarning("...", e)
   *       }
   *       context.barrier()
   *       iter
   *   }
   * }}}
   */
  @Experimental
  @Since("2.4.0")
  def barrier(): Unit = runBarrier("", RequestMethod.BARRIER)

  /**
   * :: Experimental ::
   * Blocks until all tasks in the same stage have reached this routine. Each task passes in
   * a message and returns with a list of all the messages passed in by each of those tasks.
   *
   * CAUTION! The allGather method requires the same precautions as the barrier method
   *
   * The message is type String rather than Array[Byte] because it is more convenient for
   * the user at the cost of worse performance.
   */
  @Experimental
  @Since("3.0.0")
  def allGather(message: String): Array[String] = runBarrier(message, RequestMethod.ALL_GATHER)

  /**
   * :: Experimental ::
   * Returns [[BarrierTaskInfo]] for all tasks in this barrier stage, ordered by partition ID.
   */
  @Experimental
  @Since("2.4.0")
  def getTaskInfos(): Array[BarrierTaskInfo] = {
    val addressesStr = Option(taskContext.getLocalProperty("addresses")).getOrElse("")
    addressesStr.split(",").map(_.trim()).map(new BarrierTaskInfo(_))
  }

  // delegate methods

  override def isCompleted(): Boolean = taskContext.isCompleted()

  override def isFailed(): Boolean = taskContext.isFailed()

  override def isInterrupted(): Boolean = taskContext.isInterrupted()

  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    taskContext.addTaskCompletionListener(listener)
    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): this.type = {
    taskContext.addTaskFailureListener(listener)
    this
  }

  override def stageId(): Int = taskContext.stageId()

  override def stageAttemptNumber(): Int = taskContext.stageAttemptNumber()

  override def partitionId(): Int = taskContext.partitionId()

  override def numPartitions(): Int = taskContext.numPartitions()

  override def attemptNumber(): Int = taskContext.attemptNumber()

  override def taskAttemptId(): Long = taskContext.taskAttemptId()

  override def getLocalProperty(key: String): String = taskContext.getLocalProperty(key)

  override def taskMetrics(): TaskMetrics = taskContext.taskMetrics()

  override def getMetricsSources(sourceName: String): Seq[Source] = {
    taskContext.getMetricsSources(sourceName)
  }

  override def cpus(): Int = taskContext.cpus()

  override def resources(): Map[String, ResourceInformation] = taskContext.resources()

  override def resourcesJMap(): java.util.Map[String, ResourceInformation] = {
    resources().asJava
  }

  override private[spark] def killTaskIfInterrupted(): Unit = taskContext.killTaskIfInterrupted()

  override private[spark] def getKillReason(): Option[String] = taskContext.getKillReason()

  override private[spark] def taskMemoryManager(): TaskMemoryManager = {
    taskContext.taskMemoryManager()
  }

  override private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    taskContext.registerAccumulator(a)
  }

  override private[spark] def setFetchFailed(fetchFailed: FetchFailedException): Unit = {
    taskContext.setFetchFailed(fetchFailed)
  }

  override private[spark] def markInterrupted(reason: String): Unit = {
    taskContext.markInterrupted(reason)
  }

  override private[spark] def markTaskFailed(error: Throwable): Unit = {
    taskContext.markTaskFailed(error)
  }

  override private[spark] def markTaskCompleted(error: Option[Throwable]): Unit = {
    taskContext.markTaskCompleted(error)
  }

  override private[spark] def fetchFailed: Option[FetchFailedException] = {
    taskContext.fetchFailed
  }

  override private[spark] def getLocalProperties: Properties = taskContext.getLocalProperties

  override private[spark] def interruptible(): Boolean = taskContext.interruptible()

  override private[spark] def pendingInterrupt(threadToInterrupt: Option[Thread], reason: String)
    : Unit = {
    taskContext.pendingInterrupt(threadToInterrupt, reason)
  }

  override private[spark] def createResourceUninterruptibly[T <: Closeable](resourceBuilder: => T)
    : T = {
    taskContext.createResourceUninterruptibly(resourceBuilder)
  }
}

@Experimental
@Since("2.4.0")
object BarrierTaskContext {
  /**
   * :: Experimental ::
   * Returns the currently active BarrierTaskContext. This can be called inside of user functions to
   * access contextual information about running barrier tasks.
   */
  @Experimental
  @Since("2.4.0")
  def get(): BarrierTaskContext = TaskContext.get().asInstanceOf[BarrierTaskContext]

  private val timer = {
    val executor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "Barrier task timer for barrier() calls.")
    assert(executor.isInstanceOf[ScheduledThreadPoolExecutor])
    executor.asInstanceOf[ScheduledThreadPoolExecutor]
  }

}
