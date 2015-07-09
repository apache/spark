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

import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * An RDD that reads from checkpoint files previously written to reliable storage.
 */
private[spark] class ReliableCheckpointRDD[T: ClassTag](
    @transient sc: SparkContext,
    val checkpointPath: String)
  extends CheckpointRDD[T](sc) {

  @transient private val hadoopConf = sc.hadoopConfiguration
  @transient private val fs = new Path(checkpointPath).getFileSystem(hadoopConf)
  private val broadcastedConf = sc.broadcast(new SerializableConfiguration(hadoopConf))

  /**
   * Return the path of the checkpoint directory this RDD reads data from.
   */
  override def getCheckpointFile: Option[String] = Some(checkpointPath)

  /**
   * Return partitions described by the files in the checkpoint directory.
   *
   * Since the original RDD may belong to a prior application, there is no way to know a
   * priori the number of partitions to expect. This method assumes that the original set of
   * checkpoint files are fully preserved in a reliable storage across application lifespans.
   */
  protected override def getPartitions: Array[Partition] = {
    val cpath = new Path(checkpointPath)
    val numPartitions =
      // listStatus can throw exception if path does not exist.
      if (fs.exists(cpath)) {
        val inputFiles = fs.listStatus(cpath)
          .map(_.getPath)
          .filter(_.getName.startsWith("part-"))
          .sortBy(_.toString)
        // Fail fast if input files are invalid
        inputFiles.zipWithIndex.foreach { case (path, i) =>
          if (!path.toString.endsWith(ReliableCheckpointRDD.splitIdToFile(i))) {
            throw new SparkException(s"Invalid checkpoint file: $path")
          }
        }
        inputFiles.length
      } else {
        0
      }
    Array.tabulate(numPartitions)(i => new CheckpointRDDPartition(i))
  }

  /**
   * Return the locations of the checkpoint file associated with the given partition.
   */
  protected override def getPreferredLocations(split: Partition): Seq[String] = {
    val status = fs.getFileStatus(
      new Path(checkpointPath, ReliableCheckpointRDD.splitIdToFile(split.index)))
    val locations = fs.getFileBlockLocations(status, 0, status.getLen)
    locations.headOption.toList.flatMap(_.getHosts).filter(_ != "localhost")
  }

  /**
   * Read the content of the checkpoint file associated with the given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val file = new Path(checkpointPath, ReliableCheckpointRDD.splitIdToFile(split.index))
    ReliableCheckpointRDD.readFromFile(file, broadcastedConf, context)
  }

}

private[spark] object ReliableCheckpointRDD extends Logging {

  private def splitIdToFile(splitId: Int): String = {
    "part-%05d".format(splitId)
  }

  def writeToFile[T: ClassTag](
      path: String,
      broadcastedConf: Broadcast[SerializableConfiguration],
      blockSize: Int = -1)(ctx: TaskContext, iterator: Iterator[T]) {
    val env = SparkEnv.get
    val outputDir = new Path(path)
    val fs = outputDir.getFileSystem(broadcastedConf.value.value)

    val finalOutputName = ReliableCheckpointRDD.splitIdToFile(ctx.partitionId())
    val finalOutputPath = new Path(outputDir, finalOutputName)
    val tempOutputPath =
      new Path(outputDir, s".$finalOutputName-attempt-${ctx.attemptNumber()}")

    if (fs.exists(tempOutputPath)) {
      throw new IOException(s"Checkpoint failed: temporary path $tempOutputPath already exists")
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
    Utils.tryWithSafeFinally {
      serializeStream.writeAll(iterator)
    } {
      serializeStream.close()
    }

    if (!fs.rename(tempOutputPath, finalOutputPath)) {
      if (!fs.exists(finalOutputPath)) {
        logInfo(s"Deleting tempOutputPath $tempOutputPath")
        fs.delete(tempOutputPath, false)
        throw new IOException("Checkpoint failed: failed to save output of task: " +
          s"${ctx.attemptNumber()} and final output path does not exist")
      } else {
        // Some other copy of this task must've finished before us and renamed it
        logInfo(s"Final output path $finalOutputPath already exists; not overwriting it")
        fs.delete(tempOutputPath, false)
      }
    }
  }

  def readFromFile[T](
      path: Path,
      broadcastedConf: Broadcast[SerializableConfiguration],
      context: TaskContext): Iterator[T] = {
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
    val Array(cluster, hdfsPath) = args
    val sc = new SparkContext(cluster, "CheckpointRDD Test")
    val rdd = sc.makeRDD(1 to 10, 10).flatMap(x => 1 to 10000)
    val path = new Path(hdfsPath, "temp")
    val conf = SparkHadoopUtil.get.newConfiguration(new SparkConf())
    val fs = path.getFileSystem(conf)
    val broadcastedConf = sc.broadcast(new SerializableConfiguration(conf))
    sc.runJob(rdd, ReliableCheckpointRDD.writeToFile[Int](path.toString, broadcastedConf, 1024) _)
    val cpRDD = new ReliableCheckpointRDD[Int](sc, path.toString)
    assert(cpRDD.partitions.length == rdd.partitions.length, "Number of partitions is not the same")
    assert(cpRDD.collect().toList == rdd.collect().toList, "Data of partitions not the same")
    fs.delete(path, true)
  }

}
