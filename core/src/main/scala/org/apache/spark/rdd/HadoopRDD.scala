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

import java.io.EOFException

import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.NextIterator
import org.apache.hadoop.conf.{Configuration, Configurable}

/**
 * An RDD that reads a file (or multiple files) from Hadoop (e.g. files in HDFS, the local file
 * system, or S3).
 * This accepts a general, broadcasted Hadoop Configuration because those tend to remain the same
 * across multiple reads; the 'path' is the only variable that is different across new JobConfs
 * created from the Configuration.
 */
class HadoopFileRDD[K, V](
    sc: SparkContext,
    path: String,
    broadcastedConf: Broadcast[SerializableWritable[Configuration]],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int)
  extends HadoopRDD[K, V](sc, broadcastedConf, inputFormatClass, keyClass, valueClass, minSplits) {

  override def getJobConf(): JobConf = {
    if (HadoopRDD.containsCachedMetadata(jobConfCacheKey)) {
      // getJobConf() has been called previously, so there is already a local cache of the JobConf
      // needed by this RDD.
      return HadoopRDD.getCachedMetadata(jobConfCacheKey).asInstanceOf[JobConf]
    } else {
      // Create a new JobConf, set the input file/directory paths to read from, and cache the
      // JobConf (i.e., in a shared hash map in the slave's JVM process that's accessible through
      // HadoopRDD.putCachedMetadata()), so that we only create one copy across multiple
      // getJobConf() calls for this RDD in the local process.
      // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary objects.
      val newJobConf = new JobConf(broadcastedConf.value.value)
      FileInputFormat.setInputPaths(newJobConf, path)
      HadoopRDD.putCachedMetadata(jobConfCacheKey, newJobConf)
      return newJobConf
    }
  }
}

/**
 * A Spark split class that wraps around a Hadoop InputSplit.
 */
private[spark] class HadoopPartition(rddId: Int, idx: Int, @transient s: InputSplit)
  extends Partition {

  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = (41 * (41 + rddId) + idx).toInt

  override val index: Int = idx
}

/**
 * A base class that provides core functionality for reading data partitions stored in Hadoop.
 */
class HadoopRDD[K, V](
    sc: SparkContext,
    broadcastedConf: Broadcast[SerializableWritable[Configuration]],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int)
  extends RDD[(K, V)](sc, Nil) with Logging {

  def this(
      sc: SparkContext,
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minSplits: Int) = {
    this(
      sc,
      sc.broadcast(new SerializableWritable(conf))
        .asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
      inputFormatClass,
      keyClass,
      valueClass,
      minSplits)
  }

  protected val jobConfCacheKey = "rdd_%d_job_conf".format(id)

  protected val inputFormatCacheKey = "rdd_%d_input_format".format(id)

  // Returns a JobConf that will be used on slaves to obtain input splits for Hadoop reads.
  protected def getJobConf(): JobConf = {
    val conf: Configuration = broadcastedConf.value.value
    if (conf.isInstanceOf[JobConf]) {
      // A user-broadcasted JobConf was provided to the HadoopRDD, so always use it.
      return conf.asInstanceOf[JobConf]
    } else if (HadoopRDD.containsCachedMetadata(jobConfCacheKey)) {
      // getJobConf() has been called previously, so there is already a local cache of the JobConf
      // needed by this RDD.
      return HadoopRDD.getCachedMetadata(jobConfCacheKey).asInstanceOf[JobConf]
    } else {
      // Create a JobConf that will be cached and used across this RDD's getJobConf() calls in the
      // local process. The local cache is accessed through HadoopRDD.putCachedMetadata().
      // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary objects.
      val newJobConf = new JobConf(broadcastedConf.value.value)
      HadoopRDD.putCachedMetadata(jobConfCacheKey, newJobConf)
      return newJobConf
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
    return newInputFormat
  }

  override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    val inputFormat = getInputFormat(jobConf)
    if (inputFormat.isInstanceOf[Configurable]) {
      inputFormat.asInstanceOf[Configurable].setConf(jobConf)
    }
    val inputSplits = inputFormat.getSplits(jobConf, minSplits)
    val array = new Array[Partition](inputSplits.size)
    for (i <- 0 until inputSplits.size) {
      array(i) = new HadoopPartition(id, i, inputSplits(i))
    }
    array
  }

  override def compute(theSplit: Partition, context: TaskContext) = {
    val iter = new NextIterator[(K, V)] {
      val split = theSplit.asInstanceOf[HadoopPartition]
      logInfo("Input split: " + split.inputSplit)
      var reader: RecordReader[K, V] = null

      val jobConf = getJobConf()
      val inputFormat = getInputFormat(jobConf)
      reader = inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)

      // Register an on-task-completion callback to close the input stream.
      context.addOnCompleteCallback{ () => closeIfNeeded() }

      val key: K = reader.createKey()
      val value: V = reader.createValue()

      override def getNext() = {
        try {
          finished = !reader.next(key, value)
        } catch {
          case eof: EOFException =>
            finished = true
        }
        (key, value)
      }

      override def close() {
        try {
          reader.close()
        } catch {
          case e: Exception => logWarning("Exception in RecordReader.close()", e)
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    // TODO: Filtering out "localhost" in case of file:// URLs
    val hadoopSplit = split.asInstanceOf[HadoopPartition]
    hadoopSplit.inputSplit.value.getLocations.filter(_ != "localhost")
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  def getConf: Configuration = getJobConf()
}

private[spark] object HadoopRDD {
  /**
   * The three methods below are helpers for accessing the local map, a property of the SparkEnv of
   * the local process.
   */
  def getCachedMetadata(key: String) = SparkEnv.get.hadoop.hadoopJobMetadata.get(key)

  def containsCachedMetadata(key: String) = SparkEnv.get.hadoop.hadoopJobMetadata.containsKey(key)

  def putCachedMetadata(key: String, value: Any) =
    SparkEnv.get.hadoop.hadoopJobMetadata.put(key, value)
}