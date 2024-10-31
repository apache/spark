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

import java.io.{FileNotFoundException, IOException}
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, FileInputFormat, FileSplit, InvalidInputException}
import org.apache.hadoop.mapreduce.task.{JobContextImpl, TaskAttemptContextImpl}

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.rdd.NewHadoopRDD.NewHadoopMapPartitionsWithSplitRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{SerializableConfiguration, ShutdownHookManager, Utils}
import org.apache.spark.util.ArrayImplicits._

private[spark] class NewHadoopPartition(
    rddId: Int,
    val index: Int,
    rawSplit: InputSplit with Writable)
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable(rawSplit)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

/**
 * :: DeveloperApi ::
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the new MapReduce API (`org.apache.hadoop.mapreduce`).
 *
 * @param sc The SparkContext to associate the RDD with.
 * @param inputFormatClass Storage format of the data to be read.
 * @param keyClass Class of the key associated with the inputFormatClass.
 * @param valueClass Class of the value associated with the inputFormatClass.
 * @param ignoreCorruptFiles Whether to ignore corrupt files.
 * @param ignoreMissingFiles Whether to ignore missing files.
 *
 * @note Instantiating this class directly is not recommended, please use
 * `org.apache.spark.SparkContext.newAPIHadoopRDD()`
 */
@DeveloperApi
class NewHadoopRDD[K, V](
    sc : SparkContext,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    @transient private val _conf: Configuration,
    ignoreCorruptFiles: Boolean,
    ignoreMissingFiles: Boolean)
  extends RDD[(K, V)](sc, Nil) with Logging {

  def this(
      sc : SparkContext,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      _conf: Configuration) = {
    this(
      sc,
      inputFormatClass,
      keyClass,
      valueClass,
      _conf,
      ignoreCorruptFiles = sc.conf.get(IGNORE_CORRUPT_FILES),
      ignoreMissingFiles = sc.conf.get(IGNORE_MISSING_FILES))
  }


  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val confBroadcast = sc.broadcast(new SerializableConfiguration(_conf))
  // private val serializableConf = new SerializableWritable(_conf)

  private val jobTrackerId: String = {
    val dateTimeFormatter =
      DateTimeFormatter
        .ofPattern("yyyyMMddHHmmss", Locale.US)
        .withZone(ZoneId.systemDefault())
    dateTimeFormatter.format(Instant.now())
  }

  @transient protected val jobId = new JobID(jobTrackerId, id)

  private val shouldCloneJobConf = sparkContext.conf.getBoolean("spark.hadoop.cloneConf", false)

  private val ignoreEmptySplits = sparkContext.conf.get(HADOOP_RDD_IGNORE_EMPTY_SPLITS)

  def getConf: Configuration = {
    val conf: Configuration = confBroadcast.value.value
    if (shouldCloneJobConf) {
      // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
      // one job modifies a configuration while another reads it (SPARK-2546, SPARK-10611).  This
      // problem occurs somewhat rarely because most jobs treat the configuration as though it's
      // immutable.  One solution, implemented here, is to clone the Configuration object.
      // Unfortunately, this clone can be very expensive.  To avoid unexpected performance
      // regressions for workloads and Hadoop versions that do not suffer from these thread-safety
      // issues, this cloning is disabled by default.
      NewHadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        // The Configuration passed in is actually a JobConf and possibly contains credentials.
        // To keep those credentials properly we have to create a new JobConf not a Configuration.
        if (conf.isInstanceOf[JobConf]) {
          new JobConf(conf)
        } else {
          new Configuration(conf)
        }
      }
    } else {
      conf
    }
  }

  override def getPartitions: Array[Partition] = {
    val inputFormat = inputFormatClass.getConstructor().newInstance()
    // setMinPartitions below will call FileInputFormat.listStatus(), which can be quite slow when
    // traversing a large number of directories and files. Parallelize it.
    _conf.setIfUnset(FileInputFormat.LIST_STATUS_NUM_THREADS,
      Runtime.getRuntime.availableProcessors().toString)
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(_conf)
      case _ =>
    }
    try {
      val allRowSplits = inputFormat.getSplits(new JobContextImpl(_conf, jobId)).asScala
      val rawSplits = if (ignoreEmptySplits) {
        allRowSplits.filter(_.getLength > 0)
      } else {
        allRowSplits
      }

      if (rawSplits.length == 1 && rawSplits(0).isInstanceOf[FileSplit]) {
        val fileSplit = rawSplits(0).asInstanceOf[FileSplit]
        val path = fileSplit.getPath
        if (fileSplit.getLength > conf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
          val codecFactory = new CompressionCodecFactory(_conf)
          if (Utils.isFileSplittable(path, codecFactory)) {
            logWarning(log"Loading one large file ${MDC(PATH, path.toString)} " +
              log"with only one partition, " +
              log"we can increase partition numbers for improving performance.")
          } else {
            logWarning(log"Loading one large unsplittable file ${MDC(PATH, path.toString)} " +
              log"with only one " +
              log"partition, because the file is compressed by unsplittable compression codec.")
          }
        }
      }

      val result = new Array[Partition](rawSplits.size)
      for (i <- rawSplits.indices) {
        result(i) =
            new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
      }
      result
    } catch {
      case e: InvalidInputException if ignoreMissingFiles =>
        logWarning(log"${MDC(PATH, _conf.get(FileInputFormat.INPUT_DIR))} " +
          log"doesn't exist and no partitions returned from this path.", e)
        Array.empty[Partition]
    }
  }

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new Iterator[(K, V)] {
      private val split = theSplit.asInstanceOf[NewHadoopPartition]
      logInfo(log"Task (TID ${MDC(TASK_ID, context.taskAttemptId())}) input split: " +
        log"${MDC(INPUT_SPLIT, split.serializableHadoopSplit)}")
      private val conf = getConf

      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // Sets InputFileBlockHolder for the file block's information
      split.serializableHadoopSplit.value match {
        case fs: FileSplit =>
          InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
        case _ =>
          InputFileBlockHolder.unset()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      private val getBytesReadCallback: Option[() => Long] =
        split.serializableHadoopSplit.value match {
          case _: FileSplit | _: CombineFileSplit =>
            Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
          case _ => None
        }

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
        }
      }

      private val format = inputFormatClass.getConstructor().newInstance()
      format match {
        case configurable: Configurable =>
          configurable.setConf(conf)
        case _ =>
      }
      private val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      private val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
      private var finished = false
      private var reader =
        try {
          Utils.tryInitializeResource(
            format.createRecordReader(split.serializableHadoopSplit.value, hadoopAttemptContext)
          ) { reader =>
            reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)
            reader
          }
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(log"Skipped missing file: ${MDC(PATH, split.serializableHadoopSplit)}", e)
            finished = true
            null
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException if !ignoreMissingFiles => throw e
          case e: IOException if ignoreCorruptFiles =>
            logWarning(
              log"Skipped the rest content in the corrupted file: " +
                log"${MDC(PATH, split.serializableHadoopSplit)}",
              e)
            finished = true
            null
        }

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener[Unit] { context =>
        // Update the bytesRead before closing is to make sure lingering bytesRead statistics in
        // this thread get correctly added.
        updateBytesRead()
        close()
      }

      private var havePair = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          try {
            finished = !reader.nextKeyValue
          } catch {
            case e: FileNotFoundException if ignoreMissingFiles =>
              logWarning(log"Skipped missing file: ${MDC(PATH, split.serializableHadoopSplit)}", e)
              finished = true
            // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
            case e: FileNotFoundException if !ignoreMissingFiles => throw e
            case e: IOException if ignoreCorruptFiles =>
              logWarning(
                log"Skipped the rest content in the corrupted file: " +
                  log"${MDC(PATH, split.serializableHadoopSplit)}",
                e)
              finished = true
          }
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

      override def next(): (K, V) = {
        if (!hasNext) {
          throw SparkCoreErrors.endOfStreamError()
        }
        havePair = false
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
          updateBytesRead()
        }
        (reader.getCurrentKey, reader.getCurrentValue)
      }

      private def close(): Unit = {
        if (reader != null) {
          InputFileBlockHolder.unset()
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
          if (getBytesReadCallback.isDefined) {
            updateBytesRead()
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
    new InterruptibleIterator(context, iter)
  }

  /** Maps over a partition, providing the InputSplit that was used as the base of the partition. */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
      f: (InputSplit, Iterator[(K, V)]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    new NewHadoopMapPartitionsWithSplitRDD(this, f, preservesPartitioning)
  }

  override def getPreferredLocations(hsplit: Partition): Seq[String] = {
    val split = hsplit.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value
    val locs = HadoopRDD.convertSplitLocationInfo(split.getLocationInfo)
    locs.getOrElse(split.getLocations.filter(_ != "localhost").toImmutableArraySeq)
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching NewHadoopRDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

}

private[spark] object NewHadoopRDD {
  /**
   * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
   * Therefore, we synchronize on this lock before calling new Configuration().
   */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

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

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[U] = {
      val partition = split.asInstanceOf[NewHadoopPartition]
      val inputSplit = partition.serializableHadoopSplit.value
      f(inputSplit, firstParent[T].iterator(split, context))
    }
  }
}
