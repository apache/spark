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

import org.apache.spark.storage.{BlockId, BlockStatus}


/**
 * A collection of fields and methods concerned with internal accumulators that represent
 * task level metrics.
 */
private[spark] object InternalAccumulator {

  import AccumulatorParam._

  // Prefixes used in names of internal task level metrics
  val METRICS_PREFIX = "internal.metrics."
  val SHUFFLE_READ_METRICS_PREFIX = METRICS_PREFIX + "shuffle.read."
  val SHUFFLE_WRITE_METRICS_PREFIX = METRICS_PREFIX + "shuffle.write."
  val OUTPUT_METRICS_PREFIX = METRICS_PREFIX + "output."
  val INPUT_METRICS_PREFIX = METRICS_PREFIX + "input."

  // Names of internal task level metrics
  val EXECUTOR_DESERIALIZE_TIME = METRICS_PREFIX + "executorDeserializeTime"
  val EXECUTOR_RUN_TIME = METRICS_PREFIX + "executorRunTime"
  val RESULT_SIZE = METRICS_PREFIX + "resultSize"
  val JVM_GC_TIME = METRICS_PREFIX + "jvmGCTime"
  val RESULT_SERIALIZATION_TIME = METRICS_PREFIX + "resultSerializationTime"
  val MEMORY_BYTES_SPILLED = METRICS_PREFIX + "memoryBytesSpilled"
  val DISK_BYTES_SPILLED = METRICS_PREFIX + "diskBytesSpilled"
  val PEAK_EXECUTION_MEMORY = METRICS_PREFIX + "peakExecutionMemory"
  val UPDATED_BLOCK_STATUSES = METRICS_PREFIX + "updatedBlockStatuses"
  val TEST_ACCUM = METRICS_PREFIX + "testAccumulator"

  // scalastyle:off

  // Names of shuffle read metrics
  object shuffleRead {
    val REMOTE_BLOCKS_FETCHED = SHUFFLE_READ_METRICS_PREFIX + "remoteBlocksFetched"
    val LOCAL_BLOCKS_FETCHED = SHUFFLE_READ_METRICS_PREFIX + "localBlocksFetched"
    val REMOTE_BYTES_READ = SHUFFLE_READ_METRICS_PREFIX + "remoteBytesRead"
    val LOCAL_BYTES_READ = SHUFFLE_READ_METRICS_PREFIX + "localBytesRead"
    val FETCH_WAIT_TIME = SHUFFLE_READ_METRICS_PREFIX + "fetchWaitTime"
    val RECORDS_READ = SHUFFLE_READ_METRICS_PREFIX + "recordsRead"
  }

  // Names of shuffle write metrics
  object shuffleWrite {
    val BYTES_WRITTEN = SHUFFLE_WRITE_METRICS_PREFIX + "bytesWritten"
    val RECORDS_WRITTEN = SHUFFLE_WRITE_METRICS_PREFIX + "recordsWritten"
    val WRITE_TIME = SHUFFLE_WRITE_METRICS_PREFIX + "writeTime"
  }

  // Names of output metrics
  object output {
    val WRITE_METHOD = OUTPUT_METRICS_PREFIX + "writeMethod"
    val BYTES_WRITTEN = OUTPUT_METRICS_PREFIX + "bytesWritten"
    val RECORDS_WRITTEN = OUTPUT_METRICS_PREFIX + "recordsWritten"
  }

  // Names of input metrics
  object input {
    val READ_METHOD = INPUT_METRICS_PREFIX + "readMethod"
    val BYTES_READ = INPUT_METRICS_PREFIX + "bytesRead"
    val RECORDS_READ = INPUT_METRICS_PREFIX + "recordsRead"
  }

  // scalastyle:on

  /**
   * Create an internal [[Accumulator]] by name, which must begin with [[METRICS_PREFIX]].
   */
  def create(name: String): Accumulator[_] = {
    require(name.startsWith(METRICS_PREFIX),
      s"internal accumulator name must start with '$METRICS_PREFIX': $name")
    getParam(name) match {
      case p @ LongAccumulatorParam => newMetric[Long](0L, name, p)
      case p @ IntAccumulatorParam => newMetric[Int](0, name, p)
      case p @ StringAccumulatorParam => newMetric[String]("", name, p)
      case p @ UpdatedBlockStatusesAccumulatorParam =>
        newMetric[Seq[(BlockId, BlockStatus)]](Seq(), name, p)
      case p => throw new IllegalArgumentException(
        s"unsupported accumulator param '${p.getClass.getSimpleName}' for metric '$name'.")
    }
  }

  /**
   * Get the [[AccumulatorParam]] associated with the internal metric name,
   * which must begin with [[METRICS_PREFIX]].
   */
  def getParam(name: String): AccumulatorParam[_] = {
    require(name.startsWith(METRICS_PREFIX),
      s"internal accumulator name must start with '$METRICS_PREFIX': $name")
    name match {
      case UPDATED_BLOCK_STATUSES => UpdatedBlockStatusesAccumulatorParam
      case shuffleRead.LOCAL_BLOCKS_FETCHED => IntAccumulatorParam
      case shuffleRead.REMOTE_BLOCKS_FETCHED => IntAccumulatorParam
      case input.READ_METHOD => StringAccumulatorParam
      case output.WRITE_METHOD => StringAccumulatorParam
      case _ => LongAccumulatorParam
    }
  }

  /**
   * Accumulators for tracking internal metrics.
   */
  def createAll(): Seq[Accumulator[_]] = {
    Seq[String](
      EXECUTOR_DESERIALIZE_TIME,
      EXECUTOR_RUN_TIME,
      RESULT_SIZE,
      JVM_GC_TIME,
      RESULT_SERIALIZATION_TIME,
      MEMORY_BYTES_SPILLED,
      DISK_BYTES_SPILLED,
      PEAK_EXECUTION_MEMORY,
      UPDATED_BLOCK_STATUSES).map(create) ++
      createShuffleReadAccums() ++
      createShuffleWriteAccums() ++
      createInputAccums() ++
      createOutputAccums() ++
      sys.props.get("spark.testing").map(_ => create(TEST_ACCUM)).toSeq
  }

  /**
   * Accumulators for tracking shuffle read metrics.
   */
  def createShuffleReadAccums(): Seq[Accumulator[_]] = {
    Seq[String](
      shuffleRead.REMOTE_BLOCKS_FETCHED,
      shuffleRead.LOCAL_BLOCKS_FETCHED,
      shuffleRead.REMOTE_BYTES_READ,
      shuffleRead.LOCAL_BYTES_READ,
      shuffleRead.FETCH_WAIT_TIME,
      shuffleRead.RECORDS_READ).map(create)
  }

  /**
   * Accumulators for tracking shuffle write metrics.
   */
  def createShuffleWriteAccums(): Seq[Accumulator[_]] = {
    Seq[String](
      shuffleWrite.BYTES_WRITTEN,
      shuffleWrite.RECORDS_WRITTEN,
      shuffleWrite.WRITE_TIME).map(create)
  }

  /**
   * Accumulators for tracking input metrics.
   */
  def createInputAccums(): Seq[Accumulator[_]] = {
    Seq[String](
      input.READ_METHOD,
      input.BYTES_READ,
      input.RECORDS_READ).map(create)
  }

  /**
   * Accumulators for tracking output metrics.
   */
  def createOutputAccums(): Seq[Accumulator[_]] = {
    Seq[String](
      output.WRITE_METHOD,
      output.BYTES_WRITTEN,
      output.RECORDS_WRITTEN).map(create)
  }

  /**
   * Accumulators for tracking internal metrics.
   *
   * These accumulators are created with the stage such that all tasks in the stage will
   * add to the same set of accumulators. We do this to report the distribution of accumulator
   * values across all tasks within each stage.
   */
  def create(sc: SparkContext): Seq[Accumulator[_]] = {
    val accums = createAll()
    accums.foreach { accum =>
      Accumulators.register(accum)
      sc.cleaner.foreach(_.registerAccumulatorForCleanup(accum))
    }
    accums
  }

  /**
   * Create a new accumulator representing an internal task metric.
   */
  private def newMetric[T](
      initialValue: T,
      name: String,
      param: AccumulatorParam[T]): Accumulator[T] = {
    new Accumulator[T](initialValue, param, Some(name), internal = true, countFailedValues = true)
  }

}
