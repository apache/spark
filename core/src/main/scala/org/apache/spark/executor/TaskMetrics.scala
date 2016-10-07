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

package org.apache.spark.executor

import java.util.{ArrayList, Collections}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.{BlockId, BlockStatus}
import org.apache.spark.util.{AccumulatorContext, AccumulatorMetadata, AccumulatorV2, LongAccumulator}


/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 *
 * This class is wrapper around a collection of internal accumulators that represent metrics
 * associated with a task. The local values of these accumulators are sent from the executor
 * to the driver when the task completes. These values are then merged into the corresponding
 * accumulator previously registered on the driver.
 *
 * The accumulator updates are also sent to the driver periodically (on executor heartbeat)
 * and when the task failed with an exception. The [[TaskMetrics]] object itself should never
 * be sent to the driver.
 */
@DeveloperApi
class TaskMetrics private[spark] () extends Serializable {
  // Each metric is internally represented as an accumulator
  private val _executorDeserializeTime = new LongAccumulator
  private val _executorDeserializeCpuTime = new LongAccumulator
  private val _executorRunTime = new LongAccumulator
  private val _executorCpuTime = new LongAccumulator
  private val _resultSize = new LongAccumulator
  private val _jvmGCTime = new LongAccumulator
  private val _resultSerializationTime = new LongAccumulator
  private val _memoryBytesSpilled = new LongAccumulator
  private val _diskBytesSpilled = new LongAccumulator
  private val _peakExecutionMemory = new LongAccumulator
  private val _updatedBlockStatuses = new BlockStatusesAccumulator

  /**
   * Time taken on the executor to deserialize this task.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime.sum

  /**
   * CPU Time taken on the executor to deserialize this task in nanoseconds.
   */
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime.sum

  /**
   * Time the executor spends actually running the task (including fetching shuffle data).
   */
  def executorRunTime: Long = _executorRunTime.sum

  /**
   * CPU Time the executor spends actually running the task
   * (including fetching shuffle data) in nanoseconds.
   */
  def executorCpuTime: Long = _executorCpuTime.sum

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult.
   */
  def resultSize: Long = _resultSize.sum

  /**
   * Amount of time the JVM spent in garbage collection while executing this task.
   */
  def jvmGCTime: Long = _jvmGCTime.sum

  /**
   * Amount of time spent serializing the task result.
   */
  def resultSerializationTime: Long = _resultSerializationTime.sum

  /**
   * The number of in-memory bytes spilled by this task.
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled.sum

  /**
   * The number of on-disk bytes spilled by this task.
   */
  def diskBytesSpilled: Long = _diskBytesSpilled.sum

  /**
   * Peak memory used by internal data structures created during shuffles, aggregations and
   * joins. The value of this accumulator should be approximately the sum of the peak sizes
   * across all such data structures created in this task. For SQL jobs, this only tracks all
   * unsafe operators and ExternalSort.
   */
  def peakExecutionMemory: Long = _peakExecutionMemory.sum

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   */
  def updatedBlockStatuses: Seq[(BlockId, BlockStatus)] = {
    // This is called on driver. All accumulator updates have a fixed value. So it's safe to use
    // `asScala` which accesses the internal values using `java.util.Iterator`.
    _updatedBlockStatuses.value.asScala
  }

  // Setters and increment-ers
  private[spark] def setExecutorDeserializeTime(v: Long): Unit =
    _executorDeserializeTime.setValue(v)
  private[spark] def setExecutorDeserializeCpuTime(v: Long): Unit =
    _executorDeserializeCpuTime.setValue(v)
  private[spark] def setExecutorRunTime(v: Long): Unit = _executorRunTime.setValue(v)
  private[spark] def setExecutorCpuTime(v: Long): Unit = _executorCpuTime.setValue(v)
  private[spark] def setResultSize(v: Long): Unit = _resultSize.setValue(v)
  private[spark] def setJvmGCTime(v: Long): Unit = _jvmGCTime.setValue(v)
  private[spark] def setResultSerializationTime(v: Long): Unit =
    _resultSerializationTime.setValue(v)
  private[spark] def incMemoryBytesSpilled(v: Long): Unit = _memoryBytesSpilled.add(v)
  private[spark] def incDiskBytesSpilled(v: Long): Unit = _diskBytesSpilled.add(v)
  private[spark] def incPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.add(v)
  private[spark] def incUpdatedBlockStatuses(v: (BlockId, BlockStatus)): Unit =
    _updatedBlockStatuses.add(v)
  private[spark] def setUpdatedBlockStatuses(v: java.util.List[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v)
  private[spark] def setUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v.asJava)

  /**
   * Metrics related to reading data from a [[org.apache.spark.rdd.HadoopRDD]] or from persisted
   * data, defined only in tasks with input.
   */
  val inputMetrics: InputMetrics = new InputMetrics()

  /**
   * Metrics related to writing data externally (e.g. to a distributed filesystem),
   * defined only in tasks with output.
   */
  val outputMetrics: OutputMetrics = new OutputMetrics()

  /**
   * Metrics related to shuffle read aggregated across all shuffle dependencies.
   * This is defined only if there are shuffle dependencies in this task.
   */
  val shuffleReadMetrics: ShuffleReadMetrics = new ShuffleReadMetrics()

  /**
   * Metrics related to shuffle write, defined only in shuffle map stages.
   */
  val shuffleWriteMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics()

  /**
   * A list of [[TempShuffleReadMetrics]], one per shuffle dependency.
   *
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a [[TempShuffleReadMetrics]]
   * for each dependency and merge these metrics before reporting them to the driver.
   */
  @transient private lazy val tempShuffleReadMetrics = new ArrayBuffer[TempShuffleReadMetrics]

  /**
   * Create a [[TempShuffleReadMetrics]] for a particular shuffle dependency.
   *
   * All usages are expected to be followed by a call to [[mergeShuffleReadMetrics]], which
   * merges the temporary values synchronously. Otherwise, all temporary data collected will
   * be lost.
   */
  private[spark] def createTempShuffleReadMetrics(): TempShuffleReadMetrics = synchronized {
    val readMetrics = new TempShuffleReadMetrics
    tempShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Merge values across all temporary [[ShuffleReadMetrics]] into `_shuffleReadMetrics`.
   * This is expected to be called on executor heartbeat and at the end of a task.
   */
  private[spark] def mergeShuffleReadMetrics(): Unit = synchronized {
    if (tempShuffleReadMetrics.nonEmpty) {
      shuffleReadMetrics.setMergeValues(tempShuffleReadMetrics)
    }
  }

  // Only used for test
  private[spark] val testAccum = sys.props.get("spark.testing").map(_ => new LongAccumulator)


  import InternalAccumulator._
  @transient private[spark] lazy val nameToAccums = LinkedHashMap(
    EXECUTOR_DESERIALIZE_TIME -> _executorDeserializeTime,
    EXECUTOR_DESERIALIZE_CPU_TIME -> _executorDeserializeCpuTime,
    EXECUTOR_RUN_TIME -> _executorRunTime,
    EXECUTOR_CPU_TIME -> _executorCpuTime,
    RESULT_SIZE -> _resultSize,
    JVM_GC_TIME -> _jvmGCTime,
    RESULT_SERIALIZATION_TIME -> _resultSerializationTime,
    MEMORY_BYTES_SPILLED -> _memoryBytesSpilled,
    DISK_BYTES_SPILLED -> _diskBytesSpilled,
    PEAK_EXECUTION_MEMORY -> _peakExecutionMemory,
    UPDATED_BLOCK_STATUSES -> _updatedBlockStatuses,
    shuffleRead.REMOTE_BLOCKS_FETCHED -> shuffleReadMetrics._remoteBlocksFetched,
    shuffleRead.LOCAL_BLOCKS_FETCHED -> shuffleReadMetrics._localBlocksFetched,
    shuffleRead.REMOTE_BYTES_READ -> shuffleReadMetrics._remoteBytesRead,
    shuffleRead.LOCAL_BYTES_READ -> shuffleReadMetrics._localBytesRead,
    shuffleRead.FETCH_WAIT_TIME -> shuffleReadMetrics._fetchWaitTime,
    shuffleRead.RECORDS_READ -> shuffleReadMetrics._recordsRead,
    shuffleWrite.BYTES_WRITTEN -> shuffleWriteMetrics._bytesWritten,
    shuffleWrite.RECORDS_WRITTEN -> shuffleWriteMetrics._recordsWritten,
    shuffleWrite.WRITE_TIME -> shuffleWriteMetrics._writeTime,
    input.BYTES_READ -> inputMetrics._bytesRead,
    input.RECORDS_READ -> inputMetrics._recordsRead,
    output.BYTES_WRITTEN -> outputMetrics._bytesWritten,
    output.RECORDS_WRITTEN -> outputMetrics._recordsWritten
  ) ++ testAccum.map(TEST_ACCUM -> _)

  @transient private[spark] lazy val internalAccums: Seq[AccumulatorV2[_, _]] =
    nameToAccums.values.toIndexedSeq

  /* ========================== *
   |        OTHER THINGS        |
   * ========================== */

  private[spark] def register(sc: SparkContext): Unit = {
    nameToAccums.foreach {
      case (name, acc) => acc.register(sc, name = Some(name), countFailedValues = true)
    }
  }

  /**
   * External accumulators registered with this task.
   */
  @transient private[spark] lazy val externalAccums = new ArrayBuffer[AccumulatorV2[_, _]]

  private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    externalAccums += a
  }

  private[spark] def accumulators(): Seq[AccumulatorV2[_, _]] = internalAccums ++ externalAccums

  /**
   * Looks for a registered accumulator by accumulator name.
   */
  private[spark] def lookForAccumulatorByName(name: String): Option[AccumulatorV2[_, _]] = {
    accumulators.find { acc =>
      acc.name.isDefined && acc.name.get == name
    }
  }
}


private[spark] object TaskMetrics extends Logging {
  import InternalAccumulator._

  /**
   * Create an empty task metrics that doesn't register its accumulators.
   */
  def empty: TaskMetrics = {
    val tm = new TaskMetrics
    tm.nameToAccums.foreach { case (name, acc) =>
      acc.metadata = AccumulatorMetadata(AccumulatorContext.newId(), Some(name), true)
    }
    tm
  }

  def registered: TaskMetrics = {
    val tm = empty
    tm.internalAccums.foreach(AccumulatorContext.register)
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of [[AccumulableInfo]], called on driver only.
   * The returned [[TaskMetrics]] is only used to get some internal metrics, we don't need to take
   * care of external accumulator info passed in.
   */
  def fromAccumulatorInfos(infos: Seq[AccumulableInfo]): TaskMetrics = {
    val tm = new TaskMetrics
    infos.filter(info => info.name.isDefined && info.update.isDefined).foreach { info =>
      val name = info.name.get
      val value = info.update.get
      if (name == UPDATED_BLOCK_STATUSES) {
        tm.setUpdatedBlockStatuses(value.asInstanceOf[java.util.List[(BlockId, BlockStatus)]])
      } else {
        tm.nameToAccums.get(name).foreach(
          _.asInstanceOf[LongAccumulator].setValue(value.asInstanceOf[Long])
        )
      }
    }
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of accumulator updates, called on driver only.
   */
  def fromAccumulators(accums: Seq[AccumulatorV2[_, _]]): TaskMetrics = {
    val tm = new TaskMetrics
    val (internalAccums, externalAccums) =
      accums.partition(a => a.name.isDefined && tm.nameToAccums.contains(a.name.get))

    internalAccums.foreach { acc =>
      val tmAcc = tm.nameToAccums(acc.name.get).asInstanceOf[AccumulatorV2[Any, Any]]
      tmAcc.metadata = acc.metadata
      tmAcc.merge(acc.asInstanceOf[AccumulatorV2[Any, Any]])
    }

    tm.externalAccums ++= externalAccums
    tm
  }
}


private[spark] class BlockStatusesAccumulator
  extends AccumulatorV2[(BlockId, BlockStatus), java.util.List[(BlockId, BlockStatus)]] {
  private val _seq = Collections.synchronizedList(new ArrayList[(BlockId, BlockStatus)]())

  override def isZero(): Boolean = _seq.isEmpty

  override def copyAndReset(): BlockStatusesAccumulator = new BlockStatusesAccumulator

  override def copy(): BlockStatusesAccumulator = {
    val newAcc = new BlockStatusesAccumulator
    newAcc._seq.addAll(_seq)
    newAcc
  }

  override def reset(): Unit = _seq.clear()

  override def add(v: (BlockId, BlockStatus)): Unit = _seq.add(v)

  override def merge(
    other: AccumulatorV2[(BlockId, BlockStatus), java.util.List[(BlockId, BlockStatus)]]): Unit = {
    other match {
      case o: BlockStatusesAccumulator => _seq.addAll(o.value)
      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def value: java.util.List[(BlockId, BlockStatus)] = _seq

  def setValue(newValue: java.util.List[(BlockId, BlockStatus)]): Unit = {
    _seq.clear()
    _seq.addAll(newValue)
  }
}
