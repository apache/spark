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

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.JobContextImpl

import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.input.WholeTextFileInputFormat

/**
 * An RDD that reads a bunch of text files in, and each text file becomes one record.
 */
private[spark] class WholeTextFileRDD(
    sc : SparkContext,
    inputFormatClass: Class[_ <: WholeTextFileInputFormat],
    keyClass: Class[Text],
    valueClass: Class[Text],
    conf: Configuration,
    minPartitions: Int)
  extends NewHadoopRDD[Text, Text](sc, inputFormatClass, keyClass, valueClass, conf) {

  override def getPartitions: Array[Partition] = {
    val conf = getConf
    // setMinPartitions below will call FileInputFormat.listStatus(), which can be quite slow when
    // traversing a large number of directories and files. Parallelize it.
    conf.setIfUnset(FileInputFormat.LIST_STATUS_NUM_THREADS,
      Runtime.getRuntime.availableProcessors().toString)
    val inputFormat = inputFormatClass.getConstructor().newInstance()
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }
    val jobContext = new JobContextImpl(conf, jobId)
    inputFormat.setMinPartitions(jobContext, minPartitions)
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }
}
