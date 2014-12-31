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
import java.io.EOFException

import scala.collection.immutable.Map
import scala.reflect.ClassTag
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.mapred.JobID
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapred.TaskID
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{DataReadMethod, InputMetrics}
import org.apache.spark.rdd.HadoopRDD.HadoopMapPartitionsWithSplitRDD
import org.apache.spark.util.{NextIterator, Utils}
import org.apache.spark.scheduler.{HostTaskLocation, HDFSCacheTaskLocation}

/**
 * A Spark split class that wraps around a Hadoop InputSplit.
 */
private[spark] class HadoopPartition(rddId: Int, idx: Int, @transient s: InputSplit)
  extends Partition {

  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx

  /**
   * Get any environment variables that should be added to the users environment when running pipes
   * @return a Map with the environment variables and corresponding values, it could be empty
   */
  def getPipeEnvVars(): Map[String, String] = {
    val envVars: Map[String, String] = if (inputSplit.value.isInstanceOf[FileSplit]) {
      val is: FileSplit = inputSplit.value.asInstanceOf[FileSplit]
      // map_input_file is deprecated in favor of mapreduce_map_input_file but set both
      // since its not removed yet
      Map("map_input_file" -> is.getPath().toString(),
        "mapreduce_map_input_file" -> is.getPath().toString())
    } else {
      Map()
    }
    envVars
  }
}

/**
 * :: DeveloperApi ::
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the older MapReduce API (`org.apache.hadoop.mapred`).
 *
 * Note: Instantiating this class directly is not recommended, please use
 * [[org.apache.spark.SparkContext.hadoopRDD()]]
 *
 * @param sc The SparkContext to associate the RDD with.
 * @param broadcastedConf A general Hadoop Configuration, or a subclass of it. If the enclosed
 *     variabe references an instance of JobConf, then that JobConf will be used for the Hadoop job.
 *     Otherwise, a new JobConf will be created on each slave using the enclosed Configuration.
 * @param initLocalJobConfFuncOpt Optional closure used to initialize any JobConf that HadoopRDD
 *     creates.
 * @param inputFormatClass Storage format of the data to be read.
 * @param keyClass Class of the key associated with the inputFormatClass.
 * @param valueClass Class of the value associated with the inputFormatClass.
 * @param minPartitions Minimum number of HadoopRDD partitions (Hadoop Splits) to generate.
 */
@DeveloperApi
class HadoopRDD[K, V](
    sc: SparkContext,
    broadcastedConf: Broadcast[SerializableWritable[Configuration]],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int)
  extends RDD[(K, V)](sc, Nil) with Logging {

  def this(
      sc: SparkContext,
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int) = {
    this(
      sc,
      sc.broadcast(new SerializableWritable(conf))
        .asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
      None /* initLocalJobConfFuncOpt */,
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions)
  }

  protected val jobConfCacheKey = "rdd_%d_job_conf".format(id)

  protected val inputFormatCacheKey = "rdd_%d_input_format".format(id)

  // used to build JobTracker ID
  private val createTime = new Date()

  private val shouldCloneJobConf = sc.conf.get("spark.hadoop.cloneConf", "false").toBoolean

  // Returns a JobConf that will be used on slaves to obtain input splits for Hadoop reads.
  protected def getJobConf(): JobConf = {
    val conf: Configuration = broadcastedConf.value.value
    if (shouldCloneJobConf) {
      // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
      // one job modifies a configuration while another reads it (SPARK-2546).  This problem occurs
      // somewhat rarely because most jobs treat the configuration as though it's immutable.  One
      // solution, implemented here, is to clone the Configuration object.  Unfortunately, this
      // clone can be very expensive.  To avoid unexpected performance regressions for workloads and
      // Hadoop versions that do not suffer from these thread-safety issues, this cloning is
      // disabled by default.
      HadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        val newJobConf = new JobConf(conf)
        if (!conf.isInstanceOf[JobConf]) {
          initLocalJobConfFuncOpt.map(f => f(newJobConf))
        }
        newJobConf
      }
    } else {
      if (conf.isInstanceOf[JobConf]) {
        logDebug("Re-using user-broadcasted JobConf")
        conf.asInstanceOf[JobConf]
      } else if (HadoopRDD.containsCachedMetadata(jobConfCacheKey)) {
        logDebug("Re-using cached JobConf")
        HadoopRDD.getCachedMetadata(jobConfCacheKey).asInstanceOf[JobConf]
      } else {
        // Create a JobConf that will be cached and used across this RDD's getJobConf() calls in the
        // local process. The local cache is accessed through HadoopRDD.putCachedMetadata().
        // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary objects.
        // Synchronize to prevent ConcurrentModificationException (SPARK-1097, HADOOP-10456).
        HadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
          logDebug("Creating new JobConf and caching it for later re-use")
          val newJobConf = new JobConf(conf)
          initLocalJobConfFuncOpt.map(f => f(newJobConf))
          HadoopRDD.putCachedMetadata(jobConfCacheKey, newJobConf)
          newJobConf
        }
      }
    }
  }

  protected def getInputFormat(conf: JobConf): InputFormat[K, V] = {
    if (HadoopRDD.containsCachedMetadata(inputFormatCacheKey)) {
      return HadoopRDD.getCachedMetadata(inputFormatCacheKey).asInstanceOf[InputFormat[K, V]]
    }
    // Once an InputFormat for this RDD is created, cache it so that only one reflection call is
    // done in each local process.
    val newInputFormat = ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[K, V]]
    if (newInputFormat.isInstanceOf[Configurable]) {
      newInputFormat.asInstanceOf[Configurable].setConf(conf)
    }
    HadoopRDD.putCachedMetadata(inputFormatCacheKey, newInputFormat)
    newInputFormat
  }

  override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    val inputFormat = getInputFormat(jobConf)
    if (inputFormat.isInstanceOf[Configurable]) {
      inputFormat.asInstanceOf[Configurable].setConf(jobConf)
    }
    val inputSplits = inputFormat.getSplits(jobConf, minPartitions)
    val array = new Array[Partition](inputSplits.size)
    for (i <- 0 until inputSplits.size) {
      array(i) = new HadoopPartition(id, i, inputSplits(i))
    }
    array
  }

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new NextIterator[(K, V)] {

      val split = theSplit.asInstanceOf[HadoopPartition]
      logInfo("Input split: " + split.inputSplit)
      val jobConf = getJobConf()

      val inputMetrics = new InputMetrics(DataReadMethod.Hadoop)
      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      val bytesReadCallback = if (split.inputSplit.value.isInstanceOf[FileSplit]) {
        SparkHadoopUtil.get.getFSBytesReadOnThreadCallback(
          split.inputSplit.value.asInstanceOf[FileSplit].getPath, jobConf)
      } else {
        None
      }
      if (bytesReadCallback.isDefined) {
        context.taskMetrics.inputMetrics = Some(inputMetrics)
      }

      var reader: RecordReader[K, V] = null
      val inputFormat = getInputFormat(jobConf)
      HadoopRDD.addLocalConfiguration(new SimpleDateFormat("yyyyMMddHHmm").format(createTime),
        context.stageId, theSplit.index, context.attemptId.toInt, jobConf)
      reader = inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener{ context => closeIfNeeded() }
      val key: K = reader.createKey()
      val value: V = reader.createValue()

      var recordsSinceMetricsUpdate = 0

      override def getNext() = {
        try {
          finished = !reader.next(key, value)
        } catch {
          case eof: EOFException =>
            finished = true
        }

        // Update bytes read metric every few records
        if (recordsSinceMetricsUpdate == HadoopRDD.RECORDS_BETWEEN_BYTES_READ_METRIC_UPDATES
            && bytesReadCallback.isDefined) {
          recordsSinceMetricsUpdate = 0
          val bytesReadFn = bytesReadCallback.get
          inputMetrics.bytesRead = bytesReadFn()
        } else {
          recordsSinceMetricsUpdate += 1
        }
        (key, value)
      }

      override def close() {
        try {
          reader.close()
          if (bytesReadCallback.isDefined) {
            val bytesReadFn = bytesReadCallback.get
            inputMetrics.bytesRead = bytesReadFn()
          } else if (split.inputSplit.value.isInstanceOf[FileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.bytesRead = split.inputSplit.value.getLength
              context.taskMetrics.inputMetrics = Some(inputMetrics)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        } catch {
          case e: Exception => {
            if (!Utils.inShutdown()) {
              logWarning("Exception in RecordReader.close()", e)
            }
          }
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }

  /** Maps over a partition, providing the InputSplit that was used as the base of the partition. */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
      f: (InputSplit, Iterator[(K, V)]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    new HadoopMapPartitionsWithSplitRDD(this, f, preservesPartitioning)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val hsplit = split.asInstanceOf[HadoopPartition].inputSplit.value
    val locs: Option[Seq[String]] = HadoopRDD.SPLIT_INFO_REFLECTIONS match {
      case Some(c) =>
        try {
          val lsplit = c.inputSplitWithLocationInfo.cast(hsplit)
          val infos = c.getLocationInfo.invoke(lsplit).asInstanceOf[Array[AnyRef]]
          Some(HadoopRDD.convertSplitLocationInfo(infos))
        } catch {
          case e: Exception =>
            logDebug("Failed to use InputSplitWithLocations.", e)
            None
        }
      case None => None
    }
    locs.getOrElse(hsplit.getLocations.filter(_ != "localhost"))
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  def getConf: Configuration = getJobConf()
}

private[spark] object HadoopRDD extends Logging {
  /**
   * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
   * Therefore, we synchronize on this lock before calling new JobConf() or new Configuration().
   */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  /** Update the input bytes read metric each time this number of records has been read */
  val RECORDS_BETWEEN_BYTES_READ_METRIC_UPDATES = 256

  /**
   * The three methods below are helpers for accessing the local map, a property of the SparkEnv of
   * the local process.
   */
  def getCachedMetadata(key: String) = SparkEnv.get.hadoopJobMetadata.get(key)

  def containsCachedMetadata(key: String) = SparkEnv.get.hadoopJobMetadata.containsKey(key)

  def putCachedMetadata(key: String, value: Any) =
    SparkEnv.get.hadoopJobMetadata.put(key, value)

  /** Add Hadoop configuration specific to a single partition and attempt. */
  def addLocalConfiguration(jobTrackerId: String, jobId: Int, splitId: Int, attemptId: Int,
                            conf: JobConf) {
    val jobID = new JobID(jobTrackerId, jobId)
    val taId = new TaskAttemptID(new TaskID(jobID, true, splitId), attemptId)

    conf.set("mapred.tip.id", taId.getTaskID.toString)
    conf.set("mapred.task.id", taId.toString)
    conf.setBoolean("mapred.task.is.map", true)
    conf.setInt("mapred.task.partition", splitId)
    conf.set("mapred.job.id", jobID.toString)
  }

  /**
   * Analogous to [[org.apache.spark.rdd.MapPartitionsRDD]], but passes in an InputSplit to
   * the given function rather than the index of the partition.
   */
  private[spark] class HadoopMapPartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
      prev: RDD[T],
      f: (InputSplit, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false)
    extends RDD[U](prev) {

    override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext) = {
      val partition = split.asInstanceOf[HadoopPartition]
      val inputSplit = partition.inputSplit.value
      f(inputSplit, firstParent[T].iterator(split, context))
    }
  }

  private[spark] class SplitInfoReflections {
    val inputSplitWithLocationInfo =
      Class.forName("org.apache.hadoop.mapred.InputSplitWithLocationInfo")
    val getLocationInfo = inputSplitWithLocationInfo.getMethod("getLocationInfo")
    val newInputSplit = Class.forName("org.apache.hadoop.mapreduce.InputSplit")
    val newGetLocationInfo = newInputSplit.getMethod("getLocationInfo")
    val splitLocationInfo = Class.forName("org.apache.hadoop.mapred.SplitLocationInfo")
    val isInMemory = splitLocationInfo.getMethod("isInMemory")
    val getLocation = splitLocationInfo.getMethod("getLocation")
  }

  private[spark] val SPLIT_INFO_REFLECTIONS: Option[SplitInfoReflections] = try {
    Some(new SplitInfoReflections)
  } catch {
    case e: Exception =>
      logDebug("SplitLocationInfo and other new Hadoop classes are " +
          "unavailable. Using the older Hadoop location info code.", e)
      None
  }

  private[spark] def convertSplitLocationInfo(infos: Array[AnyRef]): Seq[String] = {
    val out = ListBuffer[String]()
    infos.foreach { loc => {
      val locationStr = HadoopRDD.SPLIT_INFO_REFLECTIONS.get.
        getLocation.invoke(loc).asInstanceOf[String]
      if (locationStr != "localhost") {
        if (HadoopRDD.SPLIT_INFO_REFLECTIONS.get.isInMemory.
                invoke(loc).asInstanceOf[Boolean]) {
          logDebug("Partition " + locationStr + " is cached by Hadoop.")
          out += new HDFSCacheTaskLocation(locationStr).toString
        } else {
          out += new HostTaskLocation(locationStr).toString
        }
      }
    }}
    out.seq
  }
}
