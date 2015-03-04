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

package org.apache.spark.sql.hbase

import org.apache.spark._
import org.apache.spark.rdd.{RDD, ShuffledRDD, ShuffledRDDPartition}

class HBaseShuffledRDD (
    prevRdd: RDD[(HBaseRawType, Array[HBaseRawType])],
    part: Partitioner,
    @transient hbPartitions: Seq[HBasePartition] = Nil) extends ShuffledRDD(prevRdd, part){

  override def getPartitions: Array[Partition] = {
    if (hbPartitions==null || hbPartitions.isEmpty) {
      Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
    } else {
      // only to be invoked by clients
      hbPartitions.toArray
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    if (hbPartitions==null || hbPartitions.isEmpty) {
      Seq.empty
    } else {
      split.asInstanceOf[HBasePartition].server.map {
        identity[String]
      }.toSeq
    }
  }
}
