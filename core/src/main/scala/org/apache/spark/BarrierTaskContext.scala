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

import java.util.{Properties, Timer, TimerTask}

import scala.concurrent.duration._

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.source.Source
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

  // Number of tasks of the current barrier stage, a barrier() call must collect enough requests
  // from different tasks within the same barrier stage attempt to succeed.
  private lazy val numTasks = getTaskInfos().size

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
  def barrier(): Unit = {
    logInfo(s"Task $taskAttemptId from Stage $stageId(Attempt $stageAttemptNumber) has entered " +
      s"the global sync, current barrier epoch is $barrierEpoch.")
    logTrace("Current callSite: " + Utils.getCallSite())

    val startTime = System.currentTimeMillis()
    val timerTask = new TimerTask {
      override def run(): Unit = {
        logInfo(s"Task $taskAttemptId from Stage $stageId(Attempt $stageAttemptNumber) waiting " +
          s"under the global sync since $startTime, has been waiting for " +
          s"${MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)} seconds, " +
          s"current barrier epoch is $barrierEpoch.")
      }
    }
    // Log the update of global sync every 60 seconds.
    timer.schedule(timerTask, 60000, 60000)

    try {
      barrierCoordinator.askSync[Unit](
        message = RequestToSync(numTasks, stageId, stageAttemptNumber, taskAttemptId,
          barrierEpoch),
        // Set a fixed timeout for RPC here, so users shall get a SparkException thrown by
        // BarrierCoordinator on timeout, instead of RPCTimeoutException from the RPC framework.
        timeout = new RpcTimeout(365.days, "barrierTimeout"))
      barrierEpoch += 1
      logInfo(s"Task $taskAttemptId from Stage $stageId(Attempt $stageAttemptNumber) finished " +
        "global sync successfully, waited for " +
        s"${MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)} seconds, " +
        s"current barrier epoch is $barrierEpoch.")
    } catch {
      case e: SparkException =>
        logInfo(s"Task $taskAttemptId from Stage $stageId(Attempt $stageAttemptNumber) failed " +
          "to perform global sync, waited for " +
          s"${MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)} seconds, " +
          s"current barrier epoch is $barrierEpoch.")
        throw e
    } finally {
      timerTask.cancel()
      timer.purge()
    }
  }

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

  override def attemptNumber(): Int = taskContext.attemptNumber()

  override def taskAttemptId(): Long = taskContext.taskAttemptId()

  override def getLocalProperty(key: String): String = taskContext.getLocalProperty(key)

  override def taskMetrics(): TaskMetrics = taskContext.taskMetrics()

  override def getMetricsSources(sourceName: String): Seq[Source] = {
    taskContext.getMetricsSources(sourceName)
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

  private val timer = new Timer("Barrier task timer for barrier() calls.")

}
