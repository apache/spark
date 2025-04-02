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

import java.io.FileNotFoundException
import java.util.concurrent.TimeUnit

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{BUFFER_SIZE, CACHE_CHECKPOINT_PREFERRED_LOCS_EXPIRE_TIME, CHECKPOINT_COMPRESS}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * An RDD that reads from checkpoint files previously written to reliable storage.
 */
private[spark] class ReliableCheckpointRDD[T: ClassTag](
    sc: SparkContext,
    val checkpointPath: String,
    _partitioner: Option[Partitioner] = None
  ) extends CheckpointRDD[T](sc) {

  @transient private val hadoopConf = sc.hadoopConfiguration
  @transient private val cpath = new Path(checkpointPath)
  @transient private val fs = cpath.getFileSystem(hadoopConf)
  private val broadcastedConf = sc.broadcast(new SerializableConfiguration(hadoopConf))

  // Fail fast if checkpoint directory does not exist
  require(fs.exists(cpath), s"Checkpoint directory does not exist: $checkpointPath")

  /**
   * Return the path of the checkpoint directory this RDD reads data from.
   */
  override val getCheckpointFile: Option[String] = Some(checkpointPath)

  override val partitioner: Option[Partitioner] = {
    _partitioner.orElse {
      ReliableCheckpointRDD.readCheckpointedPartitionerFile(context, checkpointPath)
    }
  }

  /**
   * Return partitions described by the files in the checkpoint directory.
   *
   * Since the original RDD may belong to a prior application, there is no way to know a
   * priori the number of partitions to expect. This method assumes that the original set of
   * checkpoint files are fully preserved in a reliable storage across application lifespans.
   */
  protected override def getPartitions: Array[Partition] = {
    // listStatus can throw exception if path does not exist.
    val inputFiles = fs.listStatus(cpath)
      .map(_.getPath)
      .filter(_.getName.startsWith("part-"))
      .sortBy(_.getName.stripPrefix("part-").toInt)
    // Fail fast if input files are invalid
    inputFiles.zipWithIndex.foreach { case (path, i) =>
      if (path.getName != ReliableCheckpointRDD.checkpointFileName(i)) {
        throw SparkCoreErrors.invalidCheckpointFileError(path)
      }
    }
    Array.tabulate(inputFiles.length)(i => new CheckpointRDDPartition(i))
  }

  // Cache of preferred locations of checkpointed files.
  @transient private[spark] lazy val cachedPreferredLocations = CacheBuilder.newBuilder()
    .expireAfterWrite(
      SparkEnv.get.conf.get(CACHE_CHECKPOINT_PREFERRED_LOCS_EXPIRE_TIME).get,
      TimeUnit.MINUTES)
    .build(
      new CacheLoader[Partition, Seq[String]]() {
        override def load(split: Partition): Seq[String] = {
          getPartitionBlockLocations(split)
        }
      })

  // Returns the block locations of given partition on file system.
  private def getPartitionBlockLocations(split: Partition): Seq[String] = {
    val status = fs.getFileStatus(
      new Path(checkpointPath, ReliableCheckpointRDD.checkpointFileName(split.index)))
    val locations = fs.getFileBlockLocations(status, 0, status.getLen)
    locations.headOption.toList.flatMap(_.getHosts).filter(_ != "localhost")
  }

  private lazy val cachedExpireTime =
    SparkEnv.get.conf.get(CACHE_CHECKPOINT_PREFERRED_LOCS_EXPIRE_TIME)

  /**
   * Return the locations of the checkpoint file associated with the given partition.
   */
  protected override def getPreferredLocations(split: Partition): Seq[String] = {
    if (cachedExpireTime.isDefined && cachedExpireTime.get > 0) {
      cachedPreferredLocations.get(split)
    } else {
      getPartitionBlockLocations(split)
    }
  }

  /**
   * Read the content of the checkpoint file associated with the given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val file = new Path(checkpointPath, ReliableCheckpointRDD.checkpointFileName(split.index))
    ReliableCheckpointRDD.readCheckpointFile(file, broadcastedConf, context)
  }

}

private[spark] object ReliableCheckpointRDD extends Logging {

  /**
   * Return the checkpoint file name for the given partition.
   */
  private def checkpointFileName(partitionIndex: Int): String = {
    "part-%05d".format(partitionIndex)
  }

  private def checkpointPartitionerFileName(): String = {
    "_partitioner"
  }

  /**
   * Write RDD to checkpoint files and return a ReliableCheckpointRDD representing the RDD.
   */
  def writeRDDToCheckpointDirectory[T: ClassTag](
      originalRDD: RDD[T],
      checkpointDir: String,
      blockSize: Int = -1): ReliableCheckpointRDD[T] = {
    val checkpointStartTimeNs = System.nanoTime()

    val sc = originalRDD.sparkContext

    // Create the output path for the checkpoint
    val checkpointDirPath = new Path(checkpointDir)
    val fs = checkpointDirPath.getFileSystem(sc.hadoopConfiguration)
    if (!fs.mkdirs(checkpointDirPath)) {
      throw SparkCoreErrors.failToCreateCheckpointPathError(checkpointDirPath)
    }

    // Save to file, and reload it as an RDD
    val broadcastedConf = sc.broadcast(
      new SerializableConfiguration(sc.hadoopConfiguration))
    // TODO: This is expensive because it computes the RDD again unnecessarily (SPARK-8582)
    sc.runJob(originalRDD,
      writePartitionToCheckpointFile[T](checkpointDirPath.toString, broadcastedConf) _)

    if (originalRDD.partitioner.nonEmpty) {
      writePartitionerToCheckpointDir(sc, originalRDD.partitioner.get, checkpointDirPath)
    }

    val checkpointDurationMs =
      TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - checkpointStartTimeNs)
    logInfo(log"Checkpointing took ${MDC(TOTAL_TIME, checkpointDurationMs)} ms.")

    val newRDD = new ReliableCheckpointRDD[T](
      sc, checkpointDirPath.toString, originalRDD.partitioner)
    if (newRDD.partitions.length != originalRDD.partitions.length) {
      throw SparkCoreErrors.checkpointRDDHasDifferentNumberOfPartitionsFromOriginalRDDError(
        originalRDD.id, originalRDD.partitions.length, newRDD.id, newRDD.partitions.length)
    }
    newRDD
  }

  /**
   * Write an RDD partition's data to a checkpoint file.
   */
  def writePartitionToCheckpointFile[T: ClassTag](
      path: String,
      broadcastedConf: Broadcast[SerializableConfiguration],
      blockSize: Int = -1)(ctx: TaskContext, iterator: Iterator[T]): Unit = {
    val env = SparkEnv.get
    val outputDir = new Path(path)
    val fs = outputDir.getFileSystem(broadcastedConf.value.value)

    val finalOutputName = ReliableCheckpointRDD.checkpointFileName(ctx.partitionId())
    val finalOutputPath = new Path(outputDir, finalOutputName)
    val tempOutputPath = new Path(outputDir, s".$finalOutputName-attempt-${ctx.taskAttemptId()}")

    val bufferSize = env.conf.get(BUFFER_SIZE)

    val fileOutputStream = if (blockSize < 0) {
      val fileStream = fs.create(tempOutputPath, false, bufferSize)
      if (env.conf.get(CHECKPOINT_COMPRESS)) {
        CompressionCodec.createCodec(env.conf).compressedOutputStream(fileStream)
      } else {
        fileStream
      }
    } else {
      // This is mainly for testing purpose
      fs.create(tempOutputPath, false, bufferSize,
        fs.getDefaultReplication(fs.getWorkingDirectory), blockSize)
    }
    val serializer = env.serializer.newInstance()
    val serializeStream = serializer.serializeStream(fileOutputStream)
    Utils.tryWithSafeFinallyAndFailureCallbacks {
      serializeStream.writeAll(iterator)
    } (catchBlock = {
      val deleted = fs.delete(tempOutputPath, false)
      if (!deleted) {
        logInfo(log"Failed to delete tempOutputPath ${MDC(TEMP_OUTPUT_PATH, tempOutputPath)}.")
      }
    }, finallyBlock = {
      serializeStream.close()
    })

    if (!fs.rename(tempOutputPath, finalOutputPath)) {
      if (!fs.exists(finalOutputPath)) {
        logInfo(log"Deleting tempOutputPath ${MDC(TEMP_OUTPUT_PATH, tempOutputPath)}")
        fs.delete(tempOutputPath, false)
        throw SparkCoreErrors.checkpointFailedToSaveError(ctx.attemptNumber(), finalOutputPath)
      } else {
        // Some other copy of this task must've finished before us and renamed it
        logInfo(log"Final output path" +
          log" ${MDC(FINAL_OUTPUT_PATH, finalOutputPath)} already exists; not overwriting it")
        if (!fs.delete(tempOutputPath, false)) {
          logWarning(log"Error deleting ${MDC(PATH, tempOutputPath)}")
        }
      }
    }
  }

  /**
   * Write a partitioner to the given RDD checkpoint directory. This is done on a best-effort
   * basis; any exception while writing the partitioner is caught, logged and ignored.
   */
  private def writePartitionerToCheckpointDir(
    sc: SparkContext, partitioner: Partitioner, checkpointDirPath: Path): Unit = {
    try {
      val partitionerFilePath = new Path(checkpointDirPath, checkpointPartitionerFileName())
      val bufferSize = sc.conf.get(BUFFER_SIZE)
      val fs = partitionerFilePath.getFileSystem(sc.hadoopConfiguration)
      val fileOutputStream = fs.create(partitionerFilePath, false, bufferSize)
      val serializer = SparkEnv.get.serializer.newInstance()
      val serializeStream = serializer.serializeStream(fileOutputStream)
      Utils.tryWithSafeFinally {
        serializeStream.writeObject(partitioner)
      } {
        serializeStream.close()
      }
      logDebug(s"Written partitioner to $partitionerFilePath")
    } catch {
      case NonFatal(e) =>
        logWarning(log"Error writing partitioner ${MDC(PARTITIONER, partitioner)} to " +
          log"${MDC(PATH, checkpointDirPath)}")
    }
  }


  /**
   * Read a partitioner from the given RDD checkpoint directory, if it exists.
   * This is done on a best-effort basis; any exception while reading the partitioner is
   * caught, logged and ignored.
   */
  private def readCheckpointedPartitionerFile(
      sc: SparkContext,
      checkpointDirPath: String): Option[Partitioner] = {
    try {
      val bufferSize = sc.conf.get(BUFFER_SIZE)
      val partitionerFilePath = new Path(checkpointDirPath, checkpointPartitionerFileName())
      val fs = partitionerFilePath.getFileSystem(sc.hadoopConfiguration)
      val fileInputStream = fs.open(partitionerFilePath, bufferSize)
      val serializer = SparkEnv.get.serializer.newInstance()
      val partitioner = Utils.tryWithSafeFinally {
        val deserializeStream = serializer.deserializeStream(fileInputStream)
        Utils.tryWithSafeFinally {
          deserializeStream.readObject[Partitioner]()
        } {
          deserializeStream.close()
        }
      } {
        fileInputStream.close()
      }

      logDebug(s"Read partitioner from $partitionerFilePath")
      Some(partitioner)
    } catch {
      case e: FileNotFoundException =>
        logDebug("No partitioner file", e)
        None
      case NonFatal(e) =>
        logWarning(log"Error reading partitioner from ${MDC(PATH, checkpointDirPath)}, " +
          log"partitioner will not be recovered which may lead to performance loss", e)
        None
    }
  }

  /**
   * Read the content of the specified checkpoint file.
   */
  def readCheckpointFile[T](
      path: Path,
      broadcastedConf: Broadcast[SerializableConfiguration],
      context: TaskContext): Iterator[T] = {
    val env = SparkEnv.get
    val fs = path.getFileSystem(broadcastedConf.value.value)
    val bufferSize = env.conf.get(BUFFER_SIZE)
    val fileInputStream = {
      val fileStream = fs.open(path, bufferSize)
      if (env.conf.get(CHECKPOINT_COMPRESS)) {
        CompressionCodec.createCodec(env.conf).compressedInputStream(fileStream)
      } else {
        fileStream
      }
    }
    val serializer = env.serializer.newInstance()
    val deserializeStream = serializer.deserializeStream(fileInputStream)

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener[Unit](context => deserializeStream.close())

    deserializeStream.asIterator.asInstanceOf[Iterator[T]]
  }

}
