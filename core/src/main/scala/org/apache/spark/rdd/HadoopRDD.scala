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

import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, SparkEnv,
  TaskContext}
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
    hadoopConfBroadcast: Broadcast[SerializableWritable[Configuration]],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int)
  extends HadoopRDD[K, V](sc, inputFormatClass, keyClass, valueClass, minSplits) {

  private val jobConfCacheKey = "rdd_%d_job_conf".format(id)

  override def getJobConf(): JobConf = {
    if (HadoopRDD.containsCachedMetadata(jobConfCacheKey)) {
      return HadoopRDD.getCachedMetadata(jobConfCacheKey).asInstanceOf[JobConf]
    } else {
      val newJobConf = new JobConf(hadoopConfBroadcast.value.value)
      FileInputFormat.setInputPaths(newJobConf, path)
      HadoopRDD.putCachedMetadata(jobConfCacheKey, newJobConf)
      return newJobConf
    }
  }
}

/**
 * An RDD that reads a Hadoop dataset as specified by a JobConf (e.g. tables in HBase).
 */
class HadoopDatasetRDD[K, V](
    sc: SparkContext,
    @transient conf: JobConf,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int)
  extends HadoopRDD[K, V](sc, inputFormatClass, keyClass, valueClass, minSplits) {

  // Add necessary security credentials to the JobConf before broadcasting it.
  SparkEnv.get.hadoop.addCredentials(conf)

  // A Hadoop JobConf can be about 10 KB, which is pretty big, so broadcast it.
  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))

  override def getJobConf(): JobConf = confBroadcast.value.value
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
abstract class HadoopRDD[K, V](
    sc: SparkContext,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int)
  extends RDD[(K, V)](sc, Nil) with Logging {

  private val inputFormatCacheKey = "rdd_%d_input_format".format(id)

  // Returns a JobConf that will be used on slaves to obtain input splits for Hadoop reads.
  protected def getJobConf(): JobConf

  def getInputFormat(conf: JobConf): InputFormat[K, V] = {
    if (HadoopRDD.containsCachedMetadata(inputFormatCacheKey)) {
      return HadoopRDD.getCachedMetadata(inputFormatCacheKey).asInstanceOf[InputFormat[K, V]]
    }
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

  override def compute(theSplit: Partition, context: TaskContext) = new NextIterator[(K, V)] {
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

object HadoopRDD {
  def getCachedMetadata(key: String) = SparkEnv.get.hadoop.hadoopJobMetadata.get(key)

  def containsCachedMetadata(key: String) = SparkEnv.get.hadoop.hadoopJobMetadata.containsKey(key)

  def putCachedMetadata(key: String, value: Any) =
    SparkEnv.get.hadoop.hadoopJobMetadata.put(key, value)
}
