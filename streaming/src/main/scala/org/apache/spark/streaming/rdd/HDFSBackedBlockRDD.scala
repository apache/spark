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
package org.apache.spark.streaming.rdd

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.BlockRDD
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.streaming.util.{WriteAheadLogFileSegment, HdfsUtils, WriteAheadLogRandomReader}
import org.apache.spark._

private[streaming]
class HDFSBackedBlockRDDPartition(
  val blockId: BlockId, idx: Int, val segment: WriteAheadLogFileSegment) extends Partition {
  val index = idx
}

private[streaming]
class HDFSBackedBlockRDD[T: ClassTag](
    @transient sc: SparkContext,
    @transient hadoopConfiguration: Configuration,
    @transient override val blockIds: Array[BlockId],
    @transient val segments: Array[WriteAheadLogFileSegment],
    val storeInBlockManager: Boolean,
    val storageLevel: StorageLevel
  ) extends BlockRDD[T](sc, blockIds) {

  if (blockIds.length != segments.length) {
    throw new IllegalStateException("Number of block ids must be the same as number of segments!")
  }

  // Hadoop Configuration is not serializable, so broadcast it as a serializable.
  val broadcastedHadoopConf = sc.broadcast(new SerializableWritable(hadoopConfiguration))
    .asInstanceOf[Broadcast[SerializableWritable[Configuration]]]
  override def getPartitions: Array[Partition] = {
    assertValid()
    (0 until blockIds.size).map { i =>
      new HDFSBackedBlockRDDPartition(blockIds(i), i, segments(i))
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    assertValid()
    val hadoopConf = broadcastedHadoopConf.value.value
    val blockManager = SparkEnv.get.blockManager
    val partition = split.asInstanceOf[HDFSBackedBlockRDDPartition]
    val blockId = partition.blockId
    blockManager.get(blockId) match {
      // Data is in Block Manager, grab it from there.
      case Some(block) =>
        block.data.asInstanceOf[Iterator[T]]
      // Data not found in Block Manager, grab it from HDFS
      case None =>
        logInfo("Reading partition data from write ahead log " + partition.segment.path)
        val reader = new WriteAheadLogRandomReader(partition.segment.path, hadoopConf)
        val dataRead = reader.read(partition.segment)
        reader.close()
        // Currently, we support storing the data to BM only in serialized form and not in
        // deserialized form
        if (storeInBlockManager) {
          blockManager.putBytes(blockId, dataRead, storageLevel)
        }
        dataRead.rewind()
        blockManager.dataDeserialize(blockId, dataRead).asInstanceOf[Iterator[T]]
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val partition = split.asInstanceOf[HDFSBackedBlockRDDPartition]
    val locations = getBlockIdLocations()
    locations.getOrElse(partition.blockId,
      HdfsUtils.getBlockLocations(partition.segment.path, hadoopConfiguration)
        .getOrElse(new Array[String](0)).toSeq)
  }
}
