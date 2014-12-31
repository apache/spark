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

import java.io.IOException

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil

private[spark] class CheckpointRDDPartition(val index: Int) extends Partition {}

/**
 * This RDD represents a RDD checkpoint file (similar to HadoopRDD).
 */
private[spark]
class CheckpointRDD[T: ClassTag](sc: SparkContext, val checkpointPath: String)
  extends RDD[T](sc, Nil) {

  val broadcastedConf = sc.broadcast(new SerializableWritable(sc.hadoopConfiguration))

  @transient val fs = new Path(checkpointPath).getFileSystem(sc.hadoopConfiguration)

  override def getPartitions: Array[Partition] = {
    val cpath = new Path(checkpointPath)
    val numPartitions =
    // listStatus can throw exception if path does not exist.
    if (fs.exists(cpath)) {
      val dirContents = fs.listStatus(cpath).map(_.getPath)
      val partitionFiles = dirContents.filter(_.getName.startsWith("part-")).map(_.toString).sorted
      val numPart =  partitionFiles.size
      if (numPart > 0 && (! partitionFiles(0).endsWith(CheckpointRDD.splitIdToFile(0)) ||
          ! partitionFiles(numPart-1).endsWith(CheckpointRDD.splitIdToFile(numPart-1)))) {
        throw new SparkException("Invalid checkpoint directory: " + checkpointPath)
      }
      numPart
    } else 0

    Array.tabulate(numPartitions)(i => new CheckpointRDDPartition(i))
  }

  checkpointData = Some(new RDDCheckpointData[T](this))
  checkpointData.get.cpFile = Some(checkpointPath)

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val status = fs.getFileStatus(new Path(checkpointPath,
      CheckpointRDD.splitIdToFile(split.index)))
    val locations = fs.getFileBlockLocations(status, 0, status.getLen)
    locations.headOption.toList.flatMap(_.getHosts).filter(_ != "localhost")
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val file = new Path(checkpointPath, CheckpointRDD.splitIdToFile(split.index))
    CheckpointRDD.readFromFile(file, broadcastedConf, context)
  }

  override def checkpoint() {
    // Do nothing. CheckpointRDD should not be checkpointed.
  }
}

private[spark] object CheckpointRDD extends Logging {
  def splitIdToFile(splitId: Int): String = {
    "part-%05d".format(splitId)
  }

  def writeToFile[T: ClassTag](
      path: String,
      broadcastedConf: Broadcast[SerializableWritable[Configuration]],
      blockSize: Int = -1
    )(ctx: TaskContext, iterator: Iterator[T]) {
    val env = SparkEnv.get
    val outputDir = new Path(path)
    val fs = outputDir.getFileSystem(broadcastedConf.value.value)

    val finalOutputName = splitIdToFile(ctx.partitionId)
    val finalOutputPath = new Path(outputDir, finalOutputName)
    val tempOutputPath =
      new Path(outputDir, "." + finalOutputName + "-attempt-" + ctx.attemptNumber)

    if (fs.exists(tempOutputPath)) {
      throw new IOException("Checkpoint failed: temporary path " +
        tempOutputPath + " already exists")
    }
    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)

    val fileOutputStream = if (blockSize < 0) {
      fs.create(tempOutputPath, false, bufferSize)
    } else {
      // This is mainly for testing purpose
      fs.create(tempOutputPath, false, bufferSize, fs.getDefaultReplication, blockSize)
    }
    val serializer = env.serializer.newInstance()
    val serializeStream = serializer.serializeStream(fileOutputStream)
    serializeStream.writeAll(iterator)
    serializeStream.close()

    if (!fs.rename(tempOutputPath, finalOutputPath)) {
      if (!fs.exists(finalOutputPath)) {
        logInfo("Deleting tempOutputPath " + tempOutputPath)
        fs.delete(tempOutputPath, false)
        throw new IOException("Checkpoint failed: failed to save output of task: "
          + ctx.attemptNumber + " and final output path does not exist")
      } else {
        // Some other copy of this task must've finished before us and renamed it
        logInfo("Final output path " + finalOutputPath + " already exists; not overwriting it")
        fs.delete(tempOutputPath, false)
      }
    }
  }

  def readFromFile[T](
      path: Path,
      broadcastedConf: Broadcast[SerializableWritable[Configuration]],
      context: TaskContext
    ): Iterator[T] = {
    val env = SparkEnv.get
    val fs = path.getFileSystem(broadcastedConf.value.value)
    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
    val fileInputStream = fs.open(path, bufferSize)
    val serializer = env.serializer.newInstance()
    val deserializeStream = serializer.deserializeStream(fileInputStream)

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener(context => deserializeStream.close())

    deserializeStream.asIterator.asInstanceOf[Iterator[T]]
  }

  // Test whether CheckpointRDD generate expected number of partitions despite
  // each split file having multiple blocks. This needs to be run on a
  // cluster (mesos or standalone) using HDFS.
  def main(args: Array[String]) {
    import org.apache.spark._

    val Array(cluster, hdfsPath) = args
    val env = SparkEnv.get
    val sc = new SparkContext(cluster, "CheckpointRDD Test")
    val rdd = sc.makeRDD(1 to 10, 10).flatMap(x => 1 to 10000)
    val path = new Path(hdfsPath, "temp")
    val conf = SparkHadoopUtil.get.newConfiguration(new SparkConf())
    val fs = path.getFileSystem(conf)
    val broadcastedConf = sc.broadcast(new SerializableWritable(conf))
    sc.runJob(rdd, CheckpointRDD.writeToFile[Int](path.toString, broadcastedConf, 1024) _)
    val cpRDD = new CheckpointRDD[Int](sc, path.toString)
    assert(cpRDD.partitions.length == rdd.partitions.length, "Number of partitions is not the same")
    assert(cpRDD.collect.toList == rdd.collect.toList, "Data of partitions not the same")
    fs.delete(path, true)
  }
}
