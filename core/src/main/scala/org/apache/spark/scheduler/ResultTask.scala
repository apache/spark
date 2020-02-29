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

import java.io._
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * A task that sends back the output to the driver application.
 *
 * See [[Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcasted version of the serialized RDD and the function to apply on each
 *                   partition of the given RDD. Once deserialized, the type should be
 *                   (RDD[T], (TaskContext, Iterator[T]) => U).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param outputId index of the task in this job (a job can launch tasks on only a subset of the
 *                 input RDD's partitions).
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 * @param isBarrier whether this task belongs to a barrier stage. Spark must launch all the tasks
 *                  at the same time for a barrier stage.
 */
private[spark] class ResultTask[T, U](
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    locs: Seq[TaskLocation],
    val outputId: Int,
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None,
    isBarrier: Boolean = false)
  extends Task[U](stageId, stageAttemptId, partition.index, localProperties, serializedTaskMetrics,
    jobId, appId, appAttemptId, isBarrier)
  with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  def createNewIterator(context: TaskContext): Iterator[T] = {
    new Iterator[T] {
      private var finished = false
      private var currentResult: ExternalChunkedRecordByteStream[T] = null

      def getNextResults(): Boolean = {
        var result: Executor.produceResult = null
        var isFinish: Boolean = false
        result = blockLinkQueue.take()
        result match {
          case f @ Executor.FailProducerException(threadId, msg) =>
            throw new SparkProducerTaskException(threadId, msg)
          case s @ Executor.SuccessRecordResult(_, value) =>
            currentResult = value.asInstanceOf[ExternalChunkedRecordByteStream[T]]
            isFinish = false
          case o @ Executor.FinishRecordResult(_, producerCtx, overFlag) =>
            setProducerContext(producerCtx)
            isFinish = overFlag
        }
        isFinish
      }

      def getKeyValueFromQueue(): Boolean = {
        val isFinished = getNextResults()
        if (isFinished) {
          return false
        }

        val isGetFinished = currentResult.hasNext
        isGetFinished
      }

      override def hasNext: Boolean = {
        if (currentResult == null) {
          finished = getKeyValueFromQueue()
        } else {
          val isFinished = currentResult.hasNext
          // queue is emtry. finished is false
          if (!isFinished) {
            currentResult.setUsed(false)
            currentResult.init()
            currentResult = null

            finished = getKeyValueFromQueue()
          } else {
            finished = true
          }
        }
        finished
      }

      override def next(): T = {
        currentResult.next()
      }
    }
  }

  def doConsumeTask(context: TaskContext): U = {
    var result: Executor.produceResult = null
    result = blockLinkQueue.take()
    val consumeResult = result.asInstanceOf[Executor.SuccessStartConsume]
    val func = consumeResult.consumeFunc
    val res = func(context, createNewIterator(context))
    res.asInstanceOf[U]
  }

  def doProduceTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    // Deserialize the RDD and the func using the broadcast variables.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, runBoby, func) = ser.deserialize[(RDD[T],
      (TaskContext, LinkedBlockingQueue[Any], Iterator[T]) => U,
      (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    val sendFunc = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    blockLinkQueue.synchronized {
      blockLinkQueue.put(new Executor.SuccessStartConsume(context.taskAttemptId(), sendFunc))
    }

    val rddIter = rdd.iterator(partition, context)
    runBoby(context, blockLinkQueue.asInstanceOf[LinkedBlockingQueue[Any]], rddIter)
  }

  override def doOriginTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTimeNs = System.nanoTime()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTimeNs = System.nanoTime() - deserializeStartTimeNs
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    func(context, rdd.iterator(partition, context))
  }

  override def runTask(context: TaskContext): U = {
    if (blockLinkQueue != null) {
      doProduceTask(context)
    } else {
      doOriginTask(context)
    }
  }

  override def runConsumeTask(context: TaskContext): U = {
    if (blockLinkQueue != null) {
      doConsumeTask(context)
    } else {
      val errMsg = "break up task block link queue is null"
      throw new SparkException(errMsg)
    }
  }

  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ResultTask(" + stageId + ", " + partitionId + ")"
}
