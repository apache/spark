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

package org.apache.spark.scheduler

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.util.Properties

import scala.collection.mutable
import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{AccumulatorV2, ByteBufferInputStream, ByteBufferOutputStream, Utils}

/**
 * A unit of execution. We have two kinds of Task's in Spark:
 *
 *  - [[org.apache.spark.scheduler.ShuffleMapTask]]
 *  - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param partitionId index of the number in the RDD
 * @param metrics a [[TaskMetrics]] that is created at driver side and sent to executor side.
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 */
private[spark] abstract class Task[T](
    val stageId: Int,
    val stageAttemptId: Int,
    val partitionId: Int,
    // The default value is only used in tests.
    val metrics: TaskMetrics = TaskMetrics.registered,
    @transient var localProperties: Properties = new Properties) extends Serializable {

  /**
   * Called by [[org.apache.spark.executor.Executor]] to run this task.
   *
   * @param taskAttemptId an identifier for this task attempt that is unique within a SparkContext.
   * @param attemptNumber how many times this task has been attempted (0 for the first attempt)
   * @return the result of the task along with updates of Accumulators.
   */
  final def run(
      taskAttemptId: Long,
      attemptNumber: Int,
      metricsSystem: MetricsSystem): T = {
    SparkEnv.get.blockManager.registerTask(taskAttemptId)
    context = new TaskContextImpl(
      stageId,
      partitionId,
      taskAttemptId,
      attemptNumber,
      taskMemoryManager,
      localProperties,
      metricsSystem,
      metrics)
    TaskContext.setTaskContext(context)
    taskThread = Thread.currentThread()
    if (_killed) {
      kill(interruptThread = false)
    }
    try {
      runTask(context)
    } catch {
      case e: Throwable =>
        // Catch all errors; run task failure callbacks, and rethrow the exception.
        try {
          context.markTaskFailed(e)
        } catch {
          case t: Throwable =>
            e.addSuppressed(t)
        }
        throw e
    } finally {
      // Call the task completion callbacks.
      context.markTaskCompleted()
      try {
        Utils.tryLogNonFatalError {
          // Release memory used by this thread for unrolling blocks
          SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP)
          SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.OFF_HEAP)
          // Notify any tasks waiting for execution memory to be freed to wake up and try to
          // acquire memory again. This makes impossible the scenario where a task sleeps forever
          // because there are no other tasks left to notify it. Since this is safe to do but may
          // not be strictly necessary, we should revisit whether we can remove this in the future.
          val memoryManager = SparkEnv.get.memoryManager
          memoryManager.synchronized { memoryManager.notifyAll() }
        }
      } finally {
        TaskContext.unset()
      }
    }
  }

  private var taskMemoryManager: TaskMemoryManager = _

  def setTaskMemoryManager(taskMemoryManager: TaskMemoryManager): Unit = {
    this.taskMemoryManager = taskMemoryManager
  }

  def runTask(context: TaskContext): T

  def preferredLocations: Seq[TaskLocation] = Nil

  // Map output tracker epoch. Will be set by TaskScheduler.
  var epoch: Long = -1

  // Task context, to be initialized in run().
  @transient protected var context: TaskContextImpl = _

  // The actual Thread on which the task is running, if any. Initialized in run().
  @volatile @transient private var taskThread: Thread = _

  // A flag to indicate whether the task is killed. This is used in case context is not yet
  // initialized when kill() is invoked.
  @volatile @transient private var _killed = false

  protected var _executorDeserializeTime: Long = 0
  protected var _executorDeserializeCpuTime: Long = 0

  /**
   * Whether the task has been killed.
   */
  def killed: Boolean = _killed

  /**
   * Returns the amount of time spent deserializing the RDD and function to be run.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime

  /**
   * Collect the latest values of accumulators used in this task. If the task failed,
   * filter out the accumulators whose values should not be included on failures.
   */
  def collectAccumulatorUpdates(taskFailed: Boolean = false): Seq[AccumulatorV2[_, _]] = {
    if (context != null) {
      context.taskMetrics.internalAccums.filter { a =>
        // RESULT_SIZE accumulator is always zero at executor, we need to send it back as its
        // value will be updated at driver side.
        // Note: internal accumulators representing task metrics always count failed values
        !a.isZero || a.name == Some(InternalAccumulator.RESULT_SIZE)
      // zero value external accumulators may still be useful, e.g. SQLMetrics, we should not filter
      // them out.
      } ++ context.taskMetrics.externalAccums.filter(a => !taskFailed || a.countFailedValues)
    } else {
      Seq.empty
    }
  }

  /**
   * Kills a task by setting the interrupted flag to true. This relies on the upper level Spark
   * code and user code to properly handle the flag. This function should be idempotent so it can
   * be called multiple times.
   * If interruptThread is true, we will also call Thread.interrupt() on the Task's executor thread.
   */
  def kill(interruptThread: Boolean) {
    _killed = true
    if (context != null) {
      context.markInterrupted()
    }
    if (interruptThread && taskThread != null) {
      taskThread.interrupt()
    }
  }
}

/**
 * Handles transmission of tasks and their dependencies, because this can be slightly tricky. We
 * need to send the list of JARs and files added to the SparkContext with each task to ensure that
 * worker nodes find out about it, but we can't make it part of the Task because the user's code in
 * the task might depend on one of the JARs. Thus we serialize each task as multiple objects, by
 * first writing out its dependencies.
 */
private[spark] object Task {
  /**
   * Serialize a task and the current app dependencies (files and JARs added to the SparkContext)
   */
  def serializeWithDependencies(
      task: Task[_],
      currentFiles: mutable.Map[String, Long],
      currentJars: mutable.Map[String, Long],
      serializer: SerializerInstance)
    : ByteBuffer = {

    val out = new ByteBufferOutputStream(4096)
    val dataOut = new DataOutputStream(out)

    // Write currentFiles
    dataOut.writeInt(currentFiles.size)
    for ((name, timestamp) <- currentFiles) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    // Write currentJars
    dataOut.writeInt(currentJars.size)
    for ((name, timestamp) <- currentJars) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    // Write the task properties separately so it is available before full task deserialization.
    val propBytes = Utils.serialize(task.localProperties)
    dataOut.writeInt(propBytes.length)
    dataOut.write(propBytes)

    // Write the task itself and finish
    dataOut.flush()
    val taskBytes = serializer.serialize(task)
    Utils.writeByteBuffer(taskBytes, out)
    out.close()
    out.toByteBuffer
  }

  /**
   * Deserialize the list of dependencies in a task serialized with serializeWithDependencies,
   * and return the task itself as a serialized ByteBuffer. The caller can then update its
   * ClassLoaders and deserialize the task.
   *
   * @return (taskFiles, taskJars, taskBytes)
   */
  def deserializeWithDependencies(serializedTask: ByteBuffer)
    : (HashMap[String, Long], HashMap[String, Long], Properties, ByteBuffer) = {

    val in = new ByteBufferInputStream(serializedTask)
    val dataIn = new DataInputStream(in)

    // Read task's files
    val taskFiles = new HashMap[String, Long]()
    val numFiles = dataIn.readInt()
    for (i <- 0 until numFiles) {
      taskFiles(dataIn.readUTF()) = dataIn.readLong()
    }

    // Read task's JARs
    val taskJars = new HashMap[String, Long]()
    val numJars = dataIn.readInt()
    for (i <- 0 until numJars) {
      taskJars(dataIn.readUTF()) = dataIn.readLong()
    }

    val propLength = dataIn.readInt()
    val propBytes = new Array[Byte](propLength)
    dataIn.readFully(propBytes, 0, propLength)
    val taskProps = Utils.deserialize[Properties](propBytes)

    // Create a sub-buffer for the rest of the data, which is the serialized Task object
    val subBuffer = serializedTask.slice()  // ByteBufferInputStream will have read just up to task
    (taskFiles, taskJars, taskProps, subBuffer)
  }
}
