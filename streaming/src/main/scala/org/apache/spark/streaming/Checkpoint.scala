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

package org.apache.spark.streaming

import java.io._
import java.util.concurrent.{ArrayBlockingQueue, RejectedExecutionException,
  ThreadPoolExecutor, TimeUnit}

import scala.concurrent.duration.FiniteDuration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{BACKUP_FILE, CHECKPOINT_FILE, CHECKPOINT_TIME, PATH, RETRY_COUNT, TEMP_FILE}
import org.apache.spark.internal.config.UI._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.streaming.scheduler.JobGenerator
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

private[streaming]
class Checkpoint(ssc: StreamingContext, val checkpointTime: Time)
  extends Logging with Serializable {
  val master = ssc.sc.master
  val framework = ssc.sc.appName
  val jars = ssc.sc.jars
  val graph = ssc.graph
  val checkpointDir = ssc.checkpointDir
  val checkpointDuration = ssc.checkpointDuration
  val pendingTimes = ssc.scheduler.getPendingTimes().toArray
  val sparkConfPairs = ssc.conf.getAll

  def createSparkConf(): SparkConf = {

    // Reload properties for the checkpoint application since user wants to set a reload property
    // or spark had changed its value and user wants to set it back.
    val propertiesToReload = List(
      "spark.yarn.app.id",
      "spark.yarn.app.attemptId",
      "spark.driver.host",
      "spark.driver.bindAddress",
      "spark.driver.port",
      "spark.master",
      "spark.ui.port",
      "spark.blockManager.port",
      "spark.kubernetes.driver.pod.name",
      "spark.kubernetes.executor.podNamePrefix",
      "spark.yarn.jars",
      "spark.yarn.keytab",
      "spark.yarn.principal",
      "spark.kerberos.keytab",
      "spark.kerberos.principal",
      UI_FILTERS.key)

    val newSparkConf = new SparkConf(loadDefaults = false).setAll(sparkConfPairs)
      .remove("spark.driver.host")
      .remove("spark.driver.bindAddress")
      .remove("spark.driver.port")
      .remove("spark.ui.port")
      .remove("spark.blockManager.port")
      .remove("spark.kubernetes.driver.pod.name")
      .remove("spark.kubernetes.executor.podNamePrefix")
    val newReloadConf = new SparkConf(loadDefaults = true)
    propertiesToReload.foreach { prop =>
      newReloadConf.getOption(prop).foreach { value =>
        newSparkConf.set(prop, value)
      }
    }

    // Add Yarn proxy filter specific configurations to the recovered SparkConf
    val filter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    val filterPrefix = s"spark.$filter.param."
    newReloadConf.getAll.foreach { case (k, v) =>
      if (k.startsWith(filterPrefix) && k.length > filterPrefix.length) {
        newSparkConf.set(k, v)
      }
    }

    newSparkConf
  }

  def validate(): Unit = {
    assert(master != null, "Checkpoint.master is null")
    assert(framework != null, "Checkpoint.framework is null")
    assert(graph != null, "Checkpoint.graph is null")
    assert(checkpointTime != null, "Checkpoint.checkpointTime is null")
    logInfo(s"Checkpoint for time $checkpointTime validated")
  }
}

private[streaming]
object Checkpoint extends Logging {
  val PREFIX = "checkpoint-"
  val REGEX = (PREFIX + """([\d]+)([\w\.]*)""").r

  /** Get the checkpoint file for the given checkpoint time */
  def checkpointFile(checkpointDir: String, checkpointTime: Time): Path = {
    new Path(checkpointDir, PREFIX + checkpointTime.milliseconds)
  }

  /** Get the checkpoint backup file for the given checkpoint time */
  def checkpointBackupFile(checkpointDir: String, checkpointTime: Time): Path = {
    new Path(checkpointDir, PREFIX + checkpointTime.milliseconds + ".bk")
  }

  /**
   * @param checkpointDir checkpoint directory to read checkpoint files from
   * @return checkpoint files from the `checkpointDir` checkpoint directory, ordered by oldest-first
   */
  def getCheckpointFiles(checkpointDir: String, fsOption: Option[FileSystem] = None): Seq[Path] = {

    def sortFunc(path1: Path, path2: Path): Boolean = {
      val (time1, bk1) = path1.getName match { case REGEX(x, y) => (x.toLong, !y.isEmpty) }
      val (time2, bk2) = path2.getName match { case REGEX(x, y) => (x.toLong, !y.isEmpty) }
      (time1 < time2) || (time1 == time2 && bk1)
    }

    val path = new Path(checkpointDir)
    val fs = fsOption.getOrElse(path.getFileSystem(SparkHadoopUtil.get.conf))
    try {
      val statuses = fs.listStatus(path)
      if (statuses != null) {
        val paths = statuses.filterNot(_.isDirectory).map(_.getPath)
        val filtered = paths.filter(p => REGEX.findFirstIn(p.getName).nonEmpty)
        filtered.sortWith(sortFunc).toImmutableArraySeq
      } else {
        logWarning(log"Listing ${MDC(PATH, path)} returned null")
        Seq.empty
      }
    } catch {
      case _: FileNotFoundException =>
        logWarning(log"Checkpoint directory ${MDC(PATH, path)} does not exist")
        Seq.empty
    }
  }

  /** Serialize the checkpoint, or throw any exception that occurs */
  def serialize(checkpoint: Checkpoint, conf: SparkConf): Array[Byte] = {
    val compressionCodec = CompressionCodec.createCodec(conf)
    val bos = new ByteArrayOutputStream()
    val zos = compressionCodec.compressedOutputStream(bos)
    val oos = new ObjectOutputStream(zos)
    Utils.tryWithSafeFinally {
      oos.writeObject(checkpoint)
    } {
      oos.close()
    }
    bos.toByteArray
  }

  /** Deserialize a checkpoint from the input stream, or throw any exception that occurs */
  def deserialize(inputStream: InputStream, conf: SparkConf): Checkpoint = {
    val compressionCodec = CompressionCodec.createCodec(conf)
    var ois: ObjectInputStreamWithLoader = null
    Utils.tryWithSafeFinally {

      // ObjectInputStream uses the last defined user-defined class loader in the stack
      // to find classes, which maybe the wrong class loader. Hence, an inherited version
      // of ObjectInputStream is used to explicitly use the current thread's default class
      // loader to find and load classes. This is a well know Java issue and has popped up
      // in other places (e.g., https://issues.apache.org/jira/browse/GROOVY-1627)
      val zis = compressionCodec.compressedInputStream(inputStream)
      ois = new ObjectInputStreamWithLoader(zis,
        Thread.currentThread().getContextClassLoader)
      val cp = ois.readObject.asInstanceOf[Checkpoint]
      cp.validate()
      cp
    } {
      if (ois != null) {
        ois.close()
      }
    }
  }
}


/**
 * Convenience class to handle the writing of graph checkpoint to file
 */
private[streaming]
class CheckpointWriter(
    jobGenerator: JobGenerator,
    conf: SparkConf,
    checkpointDir: String,
    hadoopConf: Configuration
  ) extends Logging {
  val MAX_ATTEMPTS = 3

  // Single-thread executor which rejects executions when a large amount have queued up.
  // This fails fast since this typically means the checkpoint store will never keep up, and
  // will otherwise lead to filling memory with waiting payloads of byte[] to write.
  val executor = new ThreadPoolExecutor(
    1, 1,
    0L, TimeUnit.MILLISECONDS,
    new ArrayBlockingQueue[Runnable](1000))
  val compressionCodec = CompressionCodec.createCodec(conf)
  private var stopped = false
  @volatile private[this] var fs: FileSystem = null
  @volatile private var latestCheckpointTime: Time = null

  class CheckpointWriteHandler(
      checkpointTime: Time,
      bytes: Array[Byte],
      clearCheckpointDataLater: Boolean) extends Runnable {
    def run(): Unit = {
      if (latestCheckpointTime == null || latestCheckpointTime < checkpointTime) {
        latestCheckpointTime = checkpointTime
      }
      var attempts = 0
      val startTimeNs = System.nanoTime()
      val tempFile = new Path(checkpointDir, "temp")
      // We will do checkpoint when generating a batch and completing a batch. When the processing
      // time of a batch is greater than the batch interval, checkpointing for completing an old
      // batch may run after checkpointing of a new batch. If this happens, checkpoint of an old
      // batch actually has the latest information, so we want to recovery from it. Therefore, we
      // also use the latest checkpoint time as the file name, so that we can recover from the
      // latest checkpoint file.
      //
      // Note: there is only one thread writing the checkpoint files, so we don't need to worry
      // about thread-safety.
      val checkpointFile = Checkpoint.checkpointFile(checkpointDir, latestCheckpointTime)
      val backupFile = Checkpoint.checkpointBackupFile(checkpointDir, latestCheckpointTime)

      while (attempts < MAX_ATTEMPTS && !stopped) {
        attempts += 1
        try {
          logInfo(s"Saving checkpoint for time $checkpointTime to file '$checkpointFile'")
          if (fs == null) {
            fs = new Path(checkpointDir).getFileSystem(hadoopConf)
          }
          // Write checkpoint to temp file
          fs.delete(tempFile, true) // just in case it exists
          val fos = fs.create(tempFile)
          Utils.tryWithSafeFinally {
            fos.write(bytes)
          } {
            fos.close()
          }

          // If the checkpoint file exists, back it up
          // If the backup exists as well, just delete it, otherwise rename will fail
          if (fs.exists(checkpointFile)) {
            fs.delete(backupFile, true) // just in case it exists
            if (!fs.rename(checkpointFile, backupFile)) {
              logWarning(log"Could not rename ${MDC(CHECKPOINT_FILE, checkpointFile)} to " +
                log"${MDC(BACKUP_FILE, backupFile)}")
            }
          }

          // Rename temp file to the final checkpoint file
          if (!fs.rename(tempFile, checkpointFile)) {
            logWarning(log"Could not rename ${MDC(TEMP_FILE, tempFile)} to " +
              log"${MDC(CHECKPOINT_FILE, checkpointFile)}")
          }

          // Delete old checkpoint files
          val allCheckpointFiles = Checkpoint.getCheckpointFiles(checkpointDir, Some(fs))
          if (allCheckpointFiles.size > 10) {
            allCheckpointFiles.take(allCheckpointFiles.size - 10).foreach { file =>
              logInfo(s"Deleting $file")
              fs.delete(file, true)
            }
          }

          // All done, print success
          logInfo(s"Checkpoint for time $checkpointTime saved to file '$checkpointFile'" +
            s", took ${bytes.length} bytes and " +
            s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms")
          jobGenerator.onCheckpointCompletion(checkpointTime, clearCheckpointDataLater)
          return
        } catch {
          case ioe: IOException =>
            val msg = log"Error in attempt ${MDC(RETRY_COUNT, attempts)} of writing checkpoint " +
              log"to '${MDC(CHECKPOINT_FILE, checkpointFile)}'"
            logWarning(msg, ioe)
            fs = null
        }
      }
      logWarning(log"Could not write checkpoint for time ${MDC(CHECKPOINT_TIME, checkpointTime)} " +
        log"to file '${MDC(CHECKPOINT_FILE, checkpointFile)}'")
    }
  }

  def write(checkpoint: Checkpoint, clearCheckpointDataLater: Boolean): Unit = {
    try {
      val bytes = Checkpoint.serialize(checkpoint, conf)
      executor.execute(new CheckpointWriteHandler(
        checkpoint.checkpointTime, bytes, clearCheckpointDataLater))
      logInfo(s"Submitted checkpoint of time ${checkpoint.checkpointTime} to writer queue")
    } catch {
      case rej: RejectedExecutionException =>
        logError("Could not submit checkpoint task to the thread pool executor", rej)
    }
  }

  def stop(): Unit = synchronized {
    if (stopped) return

    val startTimeNs = System.nanoTime()
    ThreadUtils.shutdown(executor, FiniteDuration(10, TimeUnit.SECONDS))
    logInfo(s"CheckpointWriter executor terminated? ${executor.isTerminated}," +
      s" waited for ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms.")
    stopped = true
  }
}


private[streaming]
object CheckpointReader extends Logging {

  /**
   * Read checkpoint files present in the given checkpoint directory. If there are no checkpoint
   * files, then return None, else try to return the latest valid checkpoint object. If no
   * checkpoint files could be read correctly, then return None.
   */
  def read(checkpointDir: String): Option[Checkpoint] = {
    read(checkpointDir, new SparkConf(), SparkHadoopUtil.get.conf, ignoreReadError = true)
  }

  /**
   * Read checkpoint files present in the given checkpoint directory. If there are no checkpoint
   * files, then return None, else try to return the latest valid checkpoint object. If no
   * checkpoint files could be read correctly, then return None (if ignoreReadError = true),
   * or throw exception (if ignoreReadError = false).
   */
  def read(
      checkpointDir: String,
      conf: SparkConf,
      hadoopConf: Configuration,
      ignoreReadError: Boolean = false): Option[Checkpoint] = {
    val checkpointPath = new Path(checkpointDir)

    val fs = checkpointPath.getFileSystem(hadoopConf)

    // Try to find the checkpoint files
    val checkpointFiles = Checkpoint.getCheckpointFiles(checkpointDir, Some(fs)).reverse
    if (checkpointFiles.isEmpty) {
      return None
    }

    // Try to read the checkpoint files in the order
    logInfo(s"Checkpoint files found: ${checkpointFiles.mkString(",")}")
    var readError: Exception = null
    checkpointFiles.foreach { file =>
      logInfo(s"Attempting to load checkpoint from file $file")
      try {
        val fis = fs.open(file)
        val cp = Checkpoint.deserialize(fis, conf)
        logInfo(s"Checkpoint successfully loaded from file $file")
        logInfo(s"Checkpoint was generated at time ${cp.checkpointTime}")
        return Some(cp)
      } catch {
        case e: Exception =>
          readError = e
          logWarning(log"Error reading checkpoint from file ${MDC(PATH, file)}", e)
      }
    }

    // If none of checkpoint files could be read, then throw exception
    if (!ignoreReadError) {
      throw new SparkException(
        s"Failed to read checkpoint from directory $checkpointPath", readError)
    }
    None
  }
}

private[streaming]
class ObjectInputStreamWithLoader(_inputStream: InputStream, loader: ClassLoader)
  extends ObjectInputStream(_inputStream) {

  override def resolveClass(desc: ObjectStreamClass): Class[_] = {
    try {
      // scalastyle:off classforname
      return Class.forName(desc.getName(), false, loader)
      // scalastyle:on classforname
    } catch {
      case e: Exception =>
    }
    super.resolveClass(desc)
  }
}
