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

package org.apache.spark.rdd

import java.text.SimpleDateFormat
import java.util.Date

import scala.reflect.ClassTag

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, FileSplit}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.sql.{SQLConf, SQLContext}
import org.apache.spark.sql.execution.datasources.parquet.UnsafeRowParquetRecordReader
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{SerializableConfiguration, ShutdownHookManager}
import org.apache.spark.{Partition => SparkPartition, _}


private[spark] class SqlNewHadoopPartition(
    rddId: Int,
    val index: Int,
    rawSplit: InputSplit with Writable)
  extends SparkPartition {

  val serializableHadoopSplit = new SerializableWritable(rawSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + index
}

/**
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the new MapReduce API (`org.apache.hadoop.mapreduce`).
 * It is based on [[org.apache.spark.rdd.NewHadoopRDD]]. It has three additions.
 * 1. A shared broadcast Hadoop Configuration.
 * 2. An optional closure `initDriverSideJobFuncOpt` that set configurations at the driver side
 *    to the shared Hadoop Configuration.
 * 3. An optional closure `initLocalJobFuncOpt` that set configurations at both the driver side
 *    and the executor side to the shared Hadoop Configuration.
 *
 * Note: This is RDD is basically a cloned version of [[org.apache.spark.rdd.NewHadoopRDD]] with
 * changes based on [[org.apache.spark.rdd.HadoopRDD]].
 */
private[spark] class SqlNewHadoopRDD[V: ClassTag](
    sqlContext: SQLContext,
    broadcastedConf: Broadcast[SerializableConfiguration],
    @transient private val initDriverSideJobFuncOpt: Option[Job => Unit],
    initLocalJobFuncOpt: Option[Job => Unit],
    inputFormatClass: Class[_ <: InputFormat[Void, V]],
    valueClass: Class[V])
    extends RDD[V](sqlContext.sparkContext, Nil)
  with SparkHadoopMapReduceUtil
  with Logging {

  protected def getJob(): Job = {
    val conf: Configuration = broadcastedConf.value.value
    // "new Job" will make a copy of the conf. Then, it is
    // safe to mutate conf properties with initLocalJobFuncOpt
    // and initDriverSideJobFuncOpt.
    val newJob = new Job(conf)
    initLocalJobFuncOpt.map(f => f(newJob))
    newJob
  }

  def getConf(isDriverSide: Boolean): Configuration = {
    val job = getJob()
    if (isDriverSide) {
      initDriverSideJobFuncOpt.map(f => f(job))
    }
    SparkHadoopUtil.get.getConfigurationFromJobContext(job)
  }

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  @transient protected val jobId = new JobID(jobTrackerId, id)

  // If true, enable using the custom RecordReader for parquet. This only works for
  // a subset of the types (no complex types).
  protected val enableUnsafeRowParquetReader: Boolean =
    sqlContext.getConf(SQLConf.PARQUET_UNSAFE_ROW_RECORD_READER_ENABLED.key).toBoolean

  override def getPartitions: Array[SparkPartition] = {
    val conf = getConf(isDriverSide = true)
    val inputFormat = inputFormatClass.newInstance
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }
    val jobContext = newJobContext(conf, jobId)
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[SparkPartition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) =
        new SqlNewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }

  override def compute(
    theSplit: SparkPartition,
    context: TaskContext): Iterator[V] = {
    val iter = new Iterator[V] {
      val split = theSplit.asInstanceOf[SqlNewHadoopPartition]
      logInfo("Input split: " + split.serializableHadoopSplit)
      val conf = getConf(isDriverSide = false)

      val inputMetrics = context.taskMetrics
        .getInputMetricsForReadMethod(DataReadMethod.Hadoop)

      // Sets the thread local variable for the file's name
      split.serializableHadoopSplit.value match {
        case fs: FileSplit => SqlNewHadoopRDDState.setInputFileName(fs.getPath.toString)
        case _ => SqlNewHadoopRDDState.unsetInputFileName()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      val bytesReadCallback = inputMetrics.bytesReadCallback.orElse {
        split.serializableHadoopSplit.value match {
          case _: FileSplit | _: CombineFileSplit =>
            SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
          case _ => None
        }
      }
      inputMetrics.setBytesReadCallback(bytesReadCallback)

      val format = inputFormatClass.newInstance
      format match {
        case configurable: Configurable =>
          configurable.setConf(conf)
        case _ =>
      }
      val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, split.index, 0)
      val hadoopAttemptContext = newTaskAttemptContext(conf, attemptId)
      private[this] var reader: RecordReader[Void, V] = null

      /**
        * If the format is ParquetInputFormat, try to create the optimized RecordReader. If this
        * fails (for example, unsupported schema), try with the normal reader.
        * TODO: plumb this through a different way?
        */
      if (enableUnsafeRowParquetReader &&
        format.getClass.getName == "org.apache.parquet.hadoop.ParquetInputFormat") {
        val parquetReader: UnsafeRowParquetRecordReader = new UnsafeRowParquetRecordReader()
        if (!parquetReader.tryInitialize(
            split.serializableHadoopSplit.value, hadoopAttemptContext)) {
          parquetReader.close()
        } else {
          reader = parquetReader.asInstanceOf[RecordReader[Void, V]]
        }
      }

      if (reader == null) {
        reader = format.createRecordReader(
          split.serializableHadoopSplit.value, hadoopAttemptContext)
        reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)
      }

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener(context => close())

      private[this] var havePair = false
      private[this] var finished = false

      override def hasNext: Boolean = {
        if (context.isInterrupted) {
          throw new TaskKilledException
        }
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          if (finished) {
            // Close and release the reader here; close() will also be called when the task
            // completes, but for tasks that read from many files, it helps to release the
            // resources early.
            close()
          }
          havePair = !finished
        }
        !finished
      }

      override def next(): V = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        reader.getCurrentValue
      }

      private def close() {
        if (reader != null) {
          SqlNewHadoopRDDState.unsetInputFileName()
          // Close the reader and release it. Note: it's very important that we don't close the
          // reader more than once, since that exposes us to MAPREDUCE-5918 when running against
          // Hadoop 1.x and older Hadoop 2.x releases. That bug can lead to non-deterministic
          // corruption issues when reading compressed input.
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (bytesReadCallback.isDefined) {
            inputMetrics.updateBytesRead()
          } else if (split.serializableHadoopSplit.value.isInstanceOf[FileSplit] ||
            split.serializableHadoopSplit.value.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(split.serializableHadoopSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        }
      }
    }
    iter
  }

  override def getPreferredLocations(hsplit: SparkPartition): Seq[String] = {
    val split = hsplit.asInstanceOf[SqlNewHadoopPartition].serializableHadoopSplit.value
    val locs = HadoopRDD.SPLIT_INFO_REFLECTIONS match {
      case Some(c) =>
        try {
          val infos = c.newGetLocationInfo.invoke(split).asInstanceOf[Array[AnyRef]]
          Some(HadoopRDD.convertSplitLocationInfo(infos))
        } catch {
          case e : Exception =>
            logDebug("Failed to use InputSplit#getLocationInfo.", e)
            None
        }
      case None => None
    }
    locs.getOrElse(split.getLocations.filter(_ != "localhost"))
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching NewHadoopRDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

  /**
   * Analogous to [[org.apache.spark.rdd.MapPartitionsRDD]], but passes in an InputSplit to
   * the given function rather than the index of the partition.
   */
  private[spark] class NewHadoopMapPartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
      prev: RDD[T],
      f: (InputSplit, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false)
    extends RDD[U](prev) {

    override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

    override def getPartitions: Array[SparkPartition] = firstParent[T].partitions

    override def compute(split: SparkPartition, context: TaskContext): Iterator[U] = {
      val partition = split.asInstanceOf[SqlNewHadoopPartition]
      val inputSplit = partition.serializableHadoopSplit.value
      f(inputSplit, firstParent[T].iterator(split, context))
    }
  }
}
