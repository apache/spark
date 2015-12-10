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

package org.apache.spark.streaming.dstream

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.rdd.ReliableCheckpointRDD
import org.apache.spark.SparkContext

/**
 * User can customize DumpFormat by extends this abstract class.
 */
abstract class DSteamDumpFormat[K, S] extends Serializable{
  // Identity have to be provided here for every DumpableDStream instance. Otherwise it's not able
  // to recognize each other during loading.
  val identity : String

  // user can customize how to dump state information
  private[streaming] def dump(rdd : RDD[(K, S)], path: String): Unit

  // user can customize how to load state information
  def load(sc: SparkContext, path: String): RDD[(K, S)]
}

/**
 * Dump RDD to HDFS which reuse the same code path as doing checkpoint
 */
class CheckpointDumpFormat[K: ClassTag, S: ClassTag](val identity: String)
  extends DSteamDumpFormat[K, S] {

  private[streaming] def dump(rdd : RDD[(K, S)], path: String): Unit = {
    val broadcastedConf = rdd.context.broadcast(
        new SerializableConfiguration(rdd.context.hadoopConfiguration))
    val dumpFunc = ReliableCheckpointRDD.writeCheckpointFile[(K, S)](path, broadcastedConf)_

    rdd.context.runJob(rdd, dumpFunc)
  }

  def load(sc: SparkContext, path: String): RDD[(K, S)] = {
    new ReliableCheckpointRDD[(K, S)](sc , path)
  }
}
