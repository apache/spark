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

package org.apache.spark.sql.hive

import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib.CombineFileSplit

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{HadoopPartition, HadoopRDD}
import org.apache.spark.sql.hive.mapred.CombineSplitInputFormat
import org.apache.spark.sql.hive.mapred.CombineSplitInputFormat.CombineSplit
import org.apache.spark.util.SerializableConfiguration


class HadoopRDDwithCombination[K, V](
    sc: SparkContext,
    broadcastedConf: Broadcast[SerializableConfiguration],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int,
    splitCombineSize: Int) extends HadoopRDD[K, V](sc,
    broadcastedConf,
    initLocalJobConfFuncOpt,
    inputFormatClass,
    keyClass,
    valueClass,
    minPartitions
) {

  override protected def getInputFormat(conf: JobConf): InputFormat[K, V] = {
    if (splitCombineSize < 0) {
      super.getInputFormat(conf)
    } else {
      new CombineSplitInputFormat(super.getInputFormat(conf), splitCombineSize)
    }
  }

  override protected def getBytesReadCallback(split: HadoopPartition) = {
    // Find a function that will return the FileSystem bytes read by this thread. Do this before
    // creating RecordReader, because RecordReader's constructor might read some bytes
    val getBytesReadCallback: Option[() => Long] = split.inputSplit.value match {
      case _: FileSplit | _: CombineFileSplit | _: CombineSplit =>
        SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
      case _ => None
    }
    getBytesReadCallback
  }
}
