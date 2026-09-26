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

import java.io.{DataInputStream, DataOutputStream, FileNotFoundException}
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.zip.CRC32

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.hadoop.fs.{FileAlreadyExistsException, Path}

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{BUFFER_SIZE, CACHE_CHECKPOINT_PREFERRED_LOCS_EXPIRE_TIME, CHECKPOINT_COMPRESS, CHECKPOINT_VERIFY_PARTITION_COUNT_ENABLED}
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
  private val broadcastedConf = SerializableConfiguration.broadcast(sc, hadoopConf)

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
   * When the directory was written by a Spark version that supports SPARK-58883, a
   * `_num_partitions` file records the expected count and is compared against the files found.
   */
  protected override def getPartitions: Array[Partition] = {
    // listStatus can throw exception if path does not exist.
    val inputFiles = fs.listStatus(cpath)
      .map(_.getPath)
      .filter(_.getName.startsWith("part-"))
      .sortBy(_.getName.stripPrefix("part-").toInt)
    // Fail fast if input files are invalid
    inputFiles.zipWithIndex.foreach { case (path, i) =>
      val expectedFileName = ReliableCheckpointRDD.checkpointFileName(i)
      if (path.getName != expectedFileName) {
        throw SparkCoreErrors.invalidCheckpointDirectoryError(path, expectedFileName)
      }
    }
    // If a partition-count metadata file is present and the integrity check is enabled,
    // verify no trailing files are missing. Directories written by earlier Spark versions
    // have no such file; a missing file is silently tolerated for backward compatibility.
    // Set spark.checkpoint.verifyPartitionCount.enabled=false to suppress this check when
    // recovering a legitimately truncated directory. See SPARK-58883.
    if (context.conf.get(CHECKPOINT_VERIFY_PARTITION_COUNT_ENABLED)) {
      ReliableCheckpointRDD.readPartitionCountFromCheckpointDir(context, checkpointPath)
        .foreach { expected =>
          if (inputFiles.length != expected) {
            throw SparkCoreErrors.checkpointTruncatedDirectoryError(
              cpath, expected, inputFiles.length)
          }
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

  private def checkpointPartitionCountFileName(): String = {
    "_num_partitions"
  }

  /**
   * On-disk layout of the `_num_partitions` file (format version 1, 13 bytes):
   * a 1-byte format version, a 4-byte big-endian Int partition count, and an 8-byte big-endian
   * CRC32 of the preceding 5 bytes. The checksum makes the payload self-validating: count bytes
   * that are corrupted to another valid Int are rejected on read instead of being compared
   * against the files found, so metadata corruption degrades to the tolerated
   * "detection inactive" path rather than a false truncation error. See SPARK-58883.
   */
  private val PARTITION_COUNT_FORMAT_VERSION: Byte = 1
  private val PARTITION_COUNT_HEADER_LENGTH: Int = 5

  private def partitionCountHeader(partitionCount: Int): Array[Byte] = {
    ByteBuffer.allocate(PARTITION_COUNT_HEADER_LENGTH)
      .put(PARTITION_COUNT_FORMAT_VERSION)
      .putInt(partitionCount)
      .array()
  }

  private def partitionCountChecksum(header: Array[Byte]): Long = {
    val crc = new CRC32()
    crc.update(header)
    crc.getValue
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
    val broadcastedConf = SerializableConfiguration.broadcast(sc)
    // TODO: This is expensive because it computes the RDD again unnecessarily (SPARK-8582)
    sc.runJob(originalRDD,
      writePartitionToCheckpointFile[T](checkpointDirPath.toString, broadcastedConf) _)

    if (originalRDD.partitioner.nonEmpty) {
      writePartitionerToCheckpointDir(sc, originalRDD.partitioner.get, checkpointDirPath)
    }
    writePartitionCountToCheckpointDir(sc, originalRDD.partitions.length, checkpointDirPath)

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

    // On HDFS, renaming onto an existing destination reports failure by returning false, which
    // is handled below. Some FileSystem implementations instead raise FileAlreadyExistsException
    // (e.g. S3A since HADOOP-16721, ABFS); treat it the same way, as it means another attempt of
    // this task has already committed the final output (SPARK-58750).
    val renamed = try {
      fs.rename(tempOutputPath, finalOutputPath)
    } catch {
      case e: FileAlreadyExistsException =>
        logDebug(log"Rename from ${MDC(TEMP_OUTPUT_PATH, tempOutputPath)} to" +
          log" ${MDC(FINAL_OUTPUT_PATH, finalOutputPath)} failed", e)
        false
    }
    if (!renamed) {
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
   * Write the partition count of the checkpointed RDD to the checkpoint directory so that
   * a later read via [[SparkContext.checkpointFile]] can detect a truncated directory.
   * The file is written atomically (temp path then rename) so a torn write leaves no partial
   * file; the payload layout is documented at [[PARTITION_COUNT_FORMAT_VERSION]].
   * This is done on a best-effort basis; any exception is caught, logged and ignored so that
   * an inability to write the file does not prevent checkpointing. See SPARK-58883.
   */
  private def writePartitionCountToCheckpointDir(
      sc: SparkContext, partitionCount: Int, checkpointDirPath: Path): Unit = {
    try {
      val countFilePath = new Path(checkpointDirPath, checkpointPartitionCountFileName())
      val tmpFilePath = new Path(
        checkpointDirPath, s".${checkpointPartitionCountFileName()}-tmp")
      val bufferSize = sc.conf.get(BUFFER_SIZE)
      val fs = countFilePath.getFileSystem(sc.hadoopConfiguration)
      // Write to a temp path first so readers never see a partial file.
      val fileOutputStream = fs.create(tmpFilePath, true, bufferSize)
      val dos = new DataOutputStream(fileOutputStream)
      Utils.tryWithSafeFinally {
        val header = partitionCountHeader(partitionCount)
        dos.write(header)
        dos.writeLong(partitionCountChecksum(header))
      } {
        dos.close()
      }
      if (!fs.rename(tmpFilePath, countFilePath)) {
        fs.delete(tmpFilePath, false)
        logWarning(
          log"Failed to rename ${MDC(TEMP_OUTPUT_PATH, tmpFilePath)} to " +
          log"${MDC(PATH, countFilePath)}, " +
          log"truncation detection will be inactive for this directory")
      } else {
        logDebug(s"Written partition count $partitionCount to $countFilePath")
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Error writing partition count to ${MDC(PATH, checkpointDirPath)}, " +
          log"truncation detection will be inactive for this directory", e)
    }
  }

  /**
   * Read the expected partition count from the checkpoint directory metadata file, if present.
   * Returns [[None]] when the file is absent (checkpoint written by an older Spark version),
   * unreadable, or fails validation (unsupported version, checksum mismatch, negative count or
   * trailing bytes), so callers must tolerate a missing value. Only a payload that passes every
   * check is authoritative and compared against the files found. See SPARK-58883.
   */
  private def readPartitionCountFromCheckpointDir(
      sc: SparkContext, checkpointDirPath: String): Option[Int] = {
    try {
      val bufferSize = sc.conf.get(BUFFER_SIZE)
      val countFilePath = new Path(checkpointDirPath, checkpointPartitionCountFileName())
      val fs = countFilePath.getFileSystem(sc.hadoopConfiguration)
      val fileInputStream = fs.open(countFilePath, bufferSize)
      val count = Utils.tryWithSafeFinally {
        val dis = new DataInputStream(fileInputStream)
        val header = new Array[Byte](PARTITION_COUNT_HEADER_LENGTH)
        dis.readFully(header)
        val version = header(0)
        if (version != PARTITION_COUNT_FORMAT_VERSION) {
          throw new IllegalArgumentException(
            s"Unsupported _num_partitions format version: $version in $countFilePath")
        }
        val expectedChecksum = dis.readLong()
        val actualChecksum = partitionCountChecksum(header)
        if (expectedChecksum != actualChecksum) {
          throw new IllegalArgumentException(
            s"Corrupted _num_partitions in $countFilePath: checksum $actualChecksum does not " +
            s"match recorded checksum $expectedChecksum")
        }
        val partitionCount = ByteBuffer.wrap(header).getInt(1)
        if (partitionCount < 0) {
          throw new IllegalArgumentException(
            s"Corrupted _num_partitions in $countFilePath: negative count $partitionCount")
        }
        if (dis.read() != -1) {
          throw new IllegalArgumentException(
            s"Corrupted _num_partitions in $countFilePath: unexpected trailing bytes")
        }
        partitionCount
      } {
        fileInputStream.close()
      }
      logDebug(s"Read partition count $count from $countFilePath")
      Some(count)
    } catch {
      case _: FileNotFoundException =>
        logDebug(s"No partition count file in $checkpointDirPath (older checkpoint)")
        None
      case NonFatal(e) =>
        logWarning(log"Error reading partition count from ${MDC(PATH, checkpointDirPath)}, " +
          log"truncation detection will be inactive for this directory", e)
        None
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
